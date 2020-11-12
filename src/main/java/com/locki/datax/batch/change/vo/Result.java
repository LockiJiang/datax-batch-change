package com.locki.datax.batch.change.vo;

import lombok.Getter;
import lombok.Setter;

/**
 * 返回前端对象
 *
 * @author jiangyang
 * @date 2020/11/10
 */
@Getter
@Setter
public class Result<T> {
    private int code;
    private String message;
    private String detail;
    private T data;

    public Result(int code, String message, String detail) {
        this.code = code;
        this.message = message;
        this.detail = detail;
    }

    public static Result fail(int code, String message, String detail) {
        return new Result(code, message, detail);
    }

    public static Result success(int code, String message, String detail) {
        return new Result(code, message, detail);
    }

    public Result setData(T data) {
        this.data = data;
        return this;
    }
}
