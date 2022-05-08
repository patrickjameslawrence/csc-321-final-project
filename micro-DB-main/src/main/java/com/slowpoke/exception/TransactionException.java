package com.slowpoke.exception;

/**
 * transcation exception
 *
 * @author zhangjw
 * @version 1.0
 */
public class TransactionException extends DbException {

    private static final long serialVersionUID = 4452542527057000815L;

    public TransactionException() {
        super();
    }

    public TransactionException(String message) {
        super(message);
    }

    public TransactionException(String message, Throwable cause) {
        super(message, cause);
    }

    public TransactionException(Throwable cause) {
        super(cause);
    }

    public TransactionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
