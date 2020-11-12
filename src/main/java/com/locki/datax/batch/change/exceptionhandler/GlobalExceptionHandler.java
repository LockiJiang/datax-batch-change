package com.locki.datax.batch.change.exceptionhandler;

import com.locki.datax.batch.change.vo.Result;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * 全局的异常处理
 *
 * @author jiangyang
 * @date 2020/11/10
 */
@ControllerAdvice
@Slf4j
public class GlobalExceptionHandler {
    /**
     * 参数校验统一异常处理
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseBody
    public Result handleBindException(MethodArgumentNotValidException ex) {
        FieldError fieldError = ex.getBindingResult().getFieldError();
        log.warn("参数校验异常:{}({})", fieldError.getDefaultMessage(), fieldError.getField());
        return Result.fail(400, fieldError.getDefaultMessage(), fieldError.getDefaultMessage());
    }
}
