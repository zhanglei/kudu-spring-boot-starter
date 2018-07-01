package org.sj4axao.stater.kudu.exception;

/**
 * @author: LiuJie
 * @version: 2018/6/29 16:40
 * @description:
 */
public class DefaultDBNotFoundException extends RuntimeException {

    public DefaultDBNotFoundException() {
        super();
    }

    public DefaultDBNotFoundException(String message) {
        super(message);
    }
}
