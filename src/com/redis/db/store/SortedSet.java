package com.redis.db.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Dual-indexed Redis Sorted Set.
 *
 * <ul>
 *   <li>{@code memberScores}: O(1) lookup from member -> score.</li>
 *   <li>{@code scoreMembers}: sorted score -> lexicographically ordered members,
 *       allowing multiple members to share the exact same score.</li>
 * </ul>
 */
public final class SortedSet {

    public record ScoreBound(double value, boolean inclusive) {}

    public enum LexBoundType { NEG_INF, POS_INF, INCLUSIVE, EXCLUSIVE }

    public record LexBound(LexBoundType type, String value) {}

    private final Map<String, Double> memberScores = new HashMap<>();
    private final TreeMap<Double, TreeSet<String>> scoreMembers = new TreeMap<>();

    public static ScoreBound parseScoreBound(String spec) {
        boolean inclusive = true;
        String raw = spec;
        if (raw.startsWith("(")) {
            inclusive = false;
            raw = raw.substring(1);
        }
        String lower = raw.toLowerCase();
        double val;
        if ("-inf".equals(lower)) {
            val = Double.NEGATIVE_INFINITY;
        } else if ("+inf".equals(lower) || "inf".equals(lower)) {
            val = Double.POSITIVE_INFINITY;
        } else {
            val = Double.parseDouble(raw);
            if (Double.isNaN(val)) {
                throw new IllegalArgumentException("min or max is not a float");
            }
        }
        if (val == 0.0) val = 0.0;
        return new ScoreBound(val, inclusive);
    }

    public static LexBound parseLexBound(String spec) {
        if ("-".equals(spec)) return new LexBound(LexBoundType.NEG_INF, null);
        if ("+".equals(spec)) return new LexBound(LexBoundType.POS_INF, null);
        if (spec.startsWith("[")) return new LexBound(LexBoundType.INCLUSIVE, spec.substring(1));
        if (spec.startsWith("(")) return new LexBound(LexBoundType.EXCLUSIVE, spec.substring(1));
        throw new IllegalArgumentException("min or max not valid string range item");
    }

    private static double normalizeScore(double score) {
        if (Double.isNaN(score)) {
            throw new IllegalArgumentException("score is not a valid float");
        }
        return score == 0.0 ? 0.0 : score;
    }

    public synchronized int add(double rawScore, String member) {
        double score = normalizeScore(rawScore);
        Double oldScore = memberScores.get(member);
        if (oldScore != null) {
            if (Double.compare(oldScore, score) == 0) {
                return 0;
            }
            TreeSet<String> oldBucket = scoreMembers.get(oldScore);
            if (oldBucket != null) {
                oldBucket.remove(member);
                if (oldBucket.isEmpty()) {
                    scoreMembers.remove(oldScore);
                }
            }
            memberScores.put(member, score);
            scoreMembers.computeIfAbsent(score, s -> new TreeSet<>()).add(member);
            return 0;
        }
        memberScores.put(member, score);
        scoreMembers.computeIfAbsent(score, s -> new TreeSet<>()).add(member);
        return 1;
    }

    public synchronized int remove(String member) {
        Double oldScore = memberScores.remove(member);
        if (oldScore == null) {
            return 0;
        }
        TreeSet<String> bucket = scoreMembers.get(oldScore);
        if (bucket != null) {
            bucket.remove(member);
            if (bucket.isEmpty()) {
                scoreMembers.remove(oldScore);
            }
        }
        return 1;
    }

    public synchronized Double score(String member) {
        return memberScores.get(member);
    }

    public synchronized int size() {
        return memberScores.size();
    }

    public synchronized boolean isEmpty() {
        return memberScores.isEmpty();
    }

    public synchronized Integer rank(String member) {
        Double score = memberScores.get(member);
        if (score == null) return null;
        int rank = 0;
        for (TreeSet<String> bucket : scoreMembers.headMap(score, false).values()) {
            rank += bucket.size();
        }
        TreeSet<String> sameScoreBucket = scoreMembers.get(score);
        if (sameScoreBucket != null) {
            rank += sameScoreBucket.headSet(member, false).size();
        }
        return rank;
    }

    public synchronized Integer revRank(String member) {
        Integer r = rank(member);
        return r == null ? null : memberScores.size() - 1 - r;
    }

    public synchronized double incrBy(double delta, String member) {
        Double current = memberScores.get(member);
        double next = (current == null ? 0.0 : current) + delta;
        if (Double.isNaN(next)) {
            throw new IllegalArgumentException("resulting score is not a number (NaN)");
        }
        add(next, member);
        return normalizeScore(next);
    }

    public synchronized List<String> range(int start, int stop, boolean withScores, boolean reverse) {
        int size = memberScores.size();
        if (size == 0) return Collections.emptyList();

        if (start < 0) start = Math.max(0, size + start);
        if (stop < 0) stop = size + stop;
        stop = Math.min(stop, size - 1);
        if (start > stop || start >= size) return Collections.emptyList();

        List<String> result = new ArrayList<>((stop - start + 1) * (withScores ? 2 : 1));
        int idx = 0;

        NavigableMap<Double, TreeSet<String>> map =
                reverse ? scoreMembers.descendingMap() : scoreMembers;

        outer:
        for (Map.Entry<Double, TreeSet<String>> entry : map.entrySet()) {
            double score = entry.getKey();
            Set<String> members = reverse ? entry.getValue().descendingSet() : entry.getValue();
            for (String member : members) {
                if (idx >= start && idx <= stop) {
                    result.add(member);
                    if (withScores) {
                        result.add(String.valueOf(score));
                    }
                }
                idx++;
                if (idx > stop) break outer;
            }
        }
        return Collections.unmodifiableList(result);
    }

    public synchronized List<String> rangeByScore(
            double min, boolean minInc, double max, boolean maxInc, boolean withScores) {
        if (min > max || (min == max && (!minInc || !maxInc))) {
            return Collections.emptyList();
        }
        NavigableMap<Double, TreeSet<String>> sub = scoreMembers.subMap(min, minInc, max, maxInc);
        List<String> result = new ArrayList<>();
        for (Map.Entry<Double, TreeSet<String>> entry : sub.entrySet()) {
            String scoreStr = String.valueOf(entry.getKey());
            for (String member : entry.getValue()) {
                result.add(member);
                if (withScores) {
                    result.add(scoreStr);
                }
            }
        }
        return Collections.unmodifiableList(result);
    }

    public synchronized int count(double min, boolean minInc, double max, boolean maxInc) {
        if (min > max || (min == max && (!minInc || !maxInc))) {
            return 0;
        }
        int total = 0;
        for (TreeSet<String> bucket : scoreMembers.subMap(min, minInc, max, maxInc).values()) {
            total += bucket.size();
        }
        return total;
    }

    public synchronized int remRangeByRank(int start, int stop) {
        int size = memberScores.size();
        if (size == 0) return 0;

        if (start < 0) start = Math.max(0, size + start);
        if (stop < 0) stop = size + stop;
        stop = Math.min(stop, size - 1);
        if (start > stop || start >= size) return 0;

        List<String> toRemove = range(start, stop, false, false);
        int removed = 0;
        for (String m : toRemove) {
            removed += remove(m);
        }
        return removed;
    }

    public synchronized int remRangeByScore(double min, boolean minInc, double max, boolean maxInc) {
        if (min > max || (min == max && (!minInc || !maxInc))) {
            return 0;
        }
        List<String> toRemove = rangeByScore(min, minInc, max, maxInc, false);
        int removed = 0;
        for (String m : toRemove) {
            removed += remove(m);
        }
        return removed;
    }

    public synchronized List<String> rangeByLex(LexBound min, LexBound max) {
        List<String> result = new ArrayList<>();
        for (TreeSet<String> bucket : scoreMembers.values()) {
            for (String member : bucket) {
                if (matchesLexMin(member, min) && matchesLexMax(member, max)) {
                    result.add(member);
                }
            }
        }
        return Collections.unmodifiableList(result);
    }

    public synchronized int lexCount(LexBound min, LexBound max) {
        int count = 0;
        for (TreeSet<String> bucket : scoreMembers.values()) {
            for (String member : bucket) {
                if (matchesLexMin(member, min) && matchesLexMax(member, max)) {
                    count++;
                }
            }
        }
        return count;
    }

    private static boolean matchesLexMin(String member, LexBound min) {
        return switch (min.type()) {
            case NEG_INF -> true;
            case POS_INF -> false;
            case INCLUSIVE -> member.compareTo(min.value()) >= 0;
            case EXCLUSIVE -> member.compareTo(min.value()) > 0;
        };
    }

    private static boolean matchesLexMax(String member, LexBound max) {
        return switch (max.type()) {
            case POS_INF -> true;
            case NEG_INF -> false;
            case INCLUSIVE -> member.compareTo(max.value()) <= 0;
            case EXCLUSIVE -> member.compareTo(max.value()) < 0;
        };
    }
}

