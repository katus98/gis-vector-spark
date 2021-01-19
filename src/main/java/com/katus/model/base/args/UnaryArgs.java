package com.katus.model.base.args;

import com.katus.entity.args.Input;
import com.katus.entity.args.Output;
import lombok.Getter;

/**
 * @author Sun Katus
 * @version 1.0, 2021-01-19
 * @since 2.0
 */
@Getter
public abstract class UnaryArgs extends Args {
    protected Input input;
    protected Output output;

    protected UnaryArgs(String[] args) {
        super(args);
        this.input = new Input(args, "");
        this.output = new Output(args, "");
    }

    @Override
    public Input[] getInputs() {
        return new Input[]{input};
    }

    @Override
    public Output[] getOutputs() {
        return new Output[]{output};
    }

    @Override
    public Boolean isValid() {
        return input.isValid() && output.isValid();
    }
}
