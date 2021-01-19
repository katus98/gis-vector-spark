package com.katus.model.base.args;

import com.katus.entity.args.Input;
import com.katus.entity.args.Output;

/**
 * @author Sun Katus
 * @version 1.0, 2021-01-19
 * @since 2.0
 */
public abstract class MultiArgs extends Args {
    protected Input[] inputs;
    protected Output[] outputs;

    public MultiArgs(String[] args, int inputNum, int outputNum) {
        super(args);
        this.inputs = new Input[inputNum];
        this.outputs = new Output[outputNum];
        for (int i = 1; i <= inputNum; i++) {
            this.inputs[i] = new Input(args, String.valueOf(i));
        }
        for (int i = 1; i <= outputNum; i++) {
            this.outputs[i] = new Output(args, String.valueOf(i));
        }
    }

    @Override
    public Input[] getInputs() {
        return inputs;
    }

    @Override
    public Output[] getOutputs() {
        return outputs;
    }

    @Override
    public Boolean isValid() {
        boolean valid = true;
        for (Input input : inputs) {
            valid = valid && input.isValid();
        }
        for (Output output : outputs) {
            valid = valid && output.isValid();
        }
        return valid;
    }
}
