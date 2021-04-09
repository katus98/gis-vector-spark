package com.katus;

import com.katus.model.gpt.args.BufferArgs;

import java.util.Map;

/**
 * @author SUN Katus
 * @version 1.0, 2021-01-19
 * @since 2.0
 */
public class ArgsTest {
    public static void main(String[] args) {
        BufferArgs bufferArgsNew = new BufferArgs(args);
        System.out.println(bufferArgsNew.toString());
        System.out.println(bufferArgsNew.getInput());
        System.out.println(bufferArgsNew.getOutput());
        System.out.println(bufferArgsNew.getInputs()[0]);
        System.out.println(bufferArgsNew.getOutputs()[0]);
        Map<String, Object> map = bufferArgsNew.getSpecialArgs();
        for (String s : map.keySet()) {
            System.out.println(s + ": " + map.get(s));
        }
    }
}
