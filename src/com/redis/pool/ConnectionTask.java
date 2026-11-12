package com.redis.pool;

import java.net.Socket;

/**
 * Wraps a single accepted TCP connection so it can be queued
 * and handed to a ConnectionPool worker.
 *
 * Responsibilities
 * ----------------
 * None beyond carrying the socket.  The pool worker that picks
 * this up will construct a RedisClientHandler, run it to
 * completion, and close the socket in a finally block.
 *
 * Why a wrapper class and not a raw Runnable?
 * -------------------------------------------
 * Keeping the socket visible lets the pool (or the accept loop)
 * close it immediately on rejection — before a handler is even
 * created — and write an error response back to the client.
 * A plain Runnable would hide the socket inside a closure,
 * making that impossible without awkward captures.
 */
public class ConnectionTask {

    private final Socket socket;

    public ConnectionTask(Socket socket) {
        this.socket = socket;
    }

    public Socket getSocket() {
        return socket;
    }
}
