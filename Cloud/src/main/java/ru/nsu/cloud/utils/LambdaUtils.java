package ru.nsu.cloud.utils;

import java.lang.invoke.*;

public class LambdaUtils {
    public static Object deserialize(SerializedLambda serializedLambda) throws Throwable {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        MethodType methodType = MethodType.methodType(serializedLambda.getImplMethodSignature().getClass());
        MethodHandle methodHandle = lookup.findStatic(Class.forName(serializedLambda.getImplClass().replace('/', '.')),
                serializedLambda.getImplMethodName(),
                methodType);
        return methodHandle.invoke();
    }
}