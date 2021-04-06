package com.katus.model.base.args;

import com.katus.entity.io.Input;
import com.katus.entity.io.Output;
import lombok.Getter;

/**
 * @author SUN Katus
 * @version 1.0, 2021-01-19
 * @since 2.0
 */
@Getter
public abstract class TernaryArgs extends Args {
    protected Input input1, input2, input3;
    protected Output output;

    protected TernaryArgs(String[] args) {
        super(args);
        this.input1 = new Input(args, "1");
        this.input2 = new Input(args, "2");
        this.input3 = new Input(args, "3");
        this.output = new Output(args);
    }

    @Override
    public Input[] getInputs() {
        return new Input[]{input1, input2, input3};
    }

    @Override
    public Output[] getOutputs() {
        return new Output[]{output};
    }

    @Override
    public Boolean isValid() {
        return input1.isValid() && input2.isValid() && input3.isValid() && output.isValid();
    }
}
