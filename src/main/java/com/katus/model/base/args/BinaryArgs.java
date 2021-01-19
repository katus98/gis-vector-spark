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
public abstract class BinaryArgs extends Args {
    protected Input input1, input2;
    protected Output output;

    public BinaryArgs(String[] args) {
        super(args);
        this.input1 = new Input(args, "1");
        this.input2 = new Input(args, "2");
        this.output = new Output(args, "");
    }

    @Override
    public Input[] getInputs() {
        return new Input[]{input1, input2};
    }

    @Override
    public Output[] getOutputs() {
        return new Output[]{output};
    }

    @Override
    public Boolean isValid() {
        return input1.isValid() && input2.isValid() && output.isValid();
    }
}
