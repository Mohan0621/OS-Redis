/**
 * Redis data-structure types.
 *
 * Each type gets its own command queue, set of consumer workers, and
 * data store partition — isolating them from each other's load.
 *
 * Used by CommandRouter to decide which DataTypePartition receives
 * an incoming command.
 */
public enum DataType {
    STRING,
    LIST,
    HASH,
    SET,
    ZSET
}
