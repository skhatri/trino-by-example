package io.trino.plugin.base.security;

import io.airlift.slice.Slice;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;

import static io.airlift.slice.SliceUtf8.toUpperCase;

public class StringFunctions {


    @Description("Converts the string to upper case")
    @ScalarFunction("to_upper")
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice upper(ConnectorSession session, @SqlType("varchar(x)") Slice slice) {
        return toUpperCase(slice);
    }
}
