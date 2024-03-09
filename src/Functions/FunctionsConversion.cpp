#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>


namespace DB
{

FunctionBasePtr createFunctionBaseCast(
    ContextPtr context
    , const ColumnsWithTypeAndName & arguments
    , const DataTypePtr & return_type
    , std::optional<CastDiagnostic> diagnostic
    , CastType cast_type)
{
    DataTypes data_types(arguments.size());

    for (size_t i = 0; i < arguments.size(); ++i)
        data_types[i] = arguments[i].type;

    auto monotonicity = MonotonicityHelper::getMonotonicityInformation(arguments.front().type, return_type.get());
    return std::make_unique<FunctionCast>(context, "CAST", std::move(monotonicity), data_types, return_type, diagnostic, cast_type);
}

REGISTER_FUNCTION(Conversion)
{
    factory.registerFunction<FunctionToUInt8>();
    factory.registerFunction<FunctionToUInt16>();
    factory.registerFunction<FunctionToUInt32>();
    factory.registerFunction<FunctionToUInt64>();
    factory.registerFunction<FunctionToUInt128>();
    factory.registerFunction<FunctionToUInt256>();
    factory.registerFunction<FunctionToInt8>();
    factory.registerFunction<FunctionToInt16>();
    factory.registerFunction<FunctionToInt32>();
    factory.registerFunction<FunctionToInt64>();
    factory.registerFunction<FunctionToInt128>();
    factory.registerFunction<FunctionToInt256>();
    factory.registerFunction<FunctionToFloat32>();
    factory.registerFunction<FunctionToFloat64>();

    factory.registerFunction<FunctionToDecimal32>();
    factory.registerFunction<FunctionToDecimal64>();
    factory.registerFunction<FunctionToDecimal128>();
    factory.registerFunction<FunctionToDecimal256>();

    factory.registerFunction<FunctionToDate>();

    /// MySQL compatibility alias. Cannot be registered as alias,
    /// because we don't want it to be normalized to toDate in queries,
    /// otherwise CREATE DICTIONARY query breaks.
    factory.registerFunction<FunctionToDate>("DATE", {}, FunctionFactory::CaseInsensitive);

    factory.registerFunction<FunctionToDate32>();
    factory.registerFunction<FunctionToDateTime>();
    factory.registerFunction<FunctionToDateTime32>();
    factory.registerFunction<FunctionToDateTime64>();
    factory.registerFunction<FunctionToUUID>();
    factory.registerFunction<FunctionToIPv4>();
    factory.registerFunction<FunctionToIPv6>();
    factory.registerFunction<FunctionToString>();

    factory.registerFunction<FunctionToUnixTimestamp>();

    factory.registerFunction<FunctionToUInt8OrZero>();
    factory.registerFunction<FunctionToUInt16OrZero>();
    factory.registerFunction<FunctionToUInt32OrZero>();
    factory.registerFunction<FunctionToUInt64OrZero>();
    factory.registerFunction<FunctionToUInt128OrZero>();
    factory.registerFunction<FunctionToUInt256OrZero>();
    factory.registerFunction<FunctionToInt8OrZero>();
    factory.registerFunction<FunctionToInt16OrZero>();
    factory.registerFunction<FunctionToInt32OrZero>();
    factory.registerFunction<FunctionToInt64OrZero>();
    factory.registerFunction<FunctionToInt128OrZero>();
    factory.registerFunction<FunctionToInt256OrZero>();
    factory.registerFunction<FunctionToFloat32OrZero>();
    factory.registerFunction<FunctionToFloat64OrZero>();
    factory.registerFunction<FunctionToDateOrZero>();
    factory.registerFunction<FunctionToDate32OrZero>();
    factory.registerFunction<FunctionToDateTimeOrZero>();
    factory.registerFunction<FunctionToDateTime64OrZero>();

    factory.registerFunction<FunctionToDecimal32OrZero>();
    factory.registerFunction<FunctionToDecimal64OrZero>();
    factory.registerFunction<FunctionToDecimal128OrZero>();
    factory.registerFunction<FunctionToDecimal256OrZero>();

    factory.registerFunction<FunctionToUUIDOrZero>();
    factory.registerFunction<FunctionToIPv4OrZero>();
    factory.registerFunction<FunctionToIPv6OrZero>();

    factory.registerFunction<FunctionToUInt8OrNull>();
    factory.registerFunction<FunctionToUInt16OrNull>();
    factory.registerFunction<FunctionToUInt32OrNull>();
    factory.registerFunction<FunctionToUInt64OrNull>();
    factory.registerFunction<FunctionToUInt128OrNull>();
    factory.registerFunction<FunctionToUInt256OrNull>();
    factory.registerFunction<FunctionToInt8OrNull>();
    factory.registerFunction<FunctionToInt16OrNull>();
    factory.registerFunction<FunctionToInt32OrNull>();
    factory.registerFunction<FunctionToInt64OrNull>();
    factory.registerFunction<FunctionToInt128OrNull>();
    factory.registerFunction<FunctionToInt256OrNull>();
    factory.registerFunction<FunctionToFloat32OrNull>();
    factory.registerFunction<FunctionToFloat64OrNull>();
    factory.registerFunction<FunctionToDateOrNull>();
    factory.registerFunction<FunctionToDate32OrNull>();
    factory.registerFunction<FunctionToDateTimeOrNull>();
    factory.registerFunction<FunctionToDateTime64OrNull>();

    factory.registerFunction<FunctionToDecimal32OrNull>();
    factory.registerFunction<FunctionToDecimal64OrNull>();
    factory.registerFunction<FunctionToDecimal128OrNull>();
    factory.registerFunction<FunctionToDecimal256OrNull>();

    factory.registerFunction<FunctionToUUIDOrNull>();
    factory.registerFunction<FunctionToIPv4OrNull>();
    factory.registerFunction<FunctionToIPv6OrNull>();

    factory.registerFunction<FunctionParseDateTimeBestEffort>();
    factory.registerFunction<FunctionParseDateTimeBestEffortOrZero>();
    factory.registerFunction<FunctionParseDateTimeBestEffortOrNull>();
    factory.registerFunction<FunctionParseDateTimeBestEffortUS>();
    factory.registerFunction<FunctionParseDateTimeBestEffortUSOrZero>();
    factory.registerFunction<FunctionParseDateTimeBestEffortUSOrNull>();
    factory.registerFunction<FunctionParseDateTime32BestEffort>();
    factory.registerFunction<FunctionParseDateTime32BestEffortOrZero>();
    factory.registerFunction<FunctionParseDateTime32BestEffortOrNull>();
    factory.registerFunction<FunctionParseDateTime64BestEffort>();
    factory.registerFunction<FunctionParseDateTime64BestEffortOrZero>();
    factory.registerFunction<FunctionParseDateTime64BestEffortOrNull>();
    factory.registerFunction<FunctionParseDateTime64BestEffortUS>();
    factory.registerFunction<FunctionParseDateTime64BestEffortUSOrZero>();
    factory.registerFunction<FunctionParseDateTime64BestEffortUSOrNull>();

    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalNanosecond, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalMicrosecond, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalMillisecond, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalSecond, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalMinute, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalHour, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalDay, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalWeek, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalMonth, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalQuarter, PositiveMonotonicity>>();
    factory.registerFunction<FunctionConvert<DataTypeInterval, NameToIntervalYear, PositiveMonotonicity>>();
}


MonotonicityHelper::MonotonicityForRange MonotonicityHelper::getMonotonicityInformation(const DataTypePtr & from_type, const IDataType * to_type)
{
    if (const auto * type = checkAndGetDataType<DataTypeUInt8>(to_type))
        return monotonicityForType(type);
    if (const auto * type = checkAndGetDataType<DataTypeUInt16>(to_type))
        return monotonicityForType(type);
    if (const auto * type = checkAndGetDataType<DataTypeUInt32>(to_type))
        return monotonicityForType(type);
    if (const auto * type = checkAndGetDataType<DataTypeUInt64>(to_type))
        return monotonicityForType(type);
    if (const auto * type = checkAndGetDataType<DataTypeUInt128>(to_type))
        return monotonicityForType(type);
    if (const auto * type = checkAndGetDataType<DataTypeUInt256>(to_type))
        return monotonicityForType(type);
    if (const auto * type = checkAndGetDataType<DataTypeInt8>(to_type))
        return monotonicityForType(type);
    if (const auto * type = checkAndGetDataType<DataTypeInt16>(to_type))
        return monotonicityForType(type);
    if (const auto * type = checkAndGetDataType<DataTypeInt32>(to_type))
        return monotonicityForType(type);
    if (const auto * type = checkAndGetDataType<DataTypeInt64>(to_type))
        return monotonicityForType(type);
    if (const auto * type = checkAndGetDataType<DataTypeInt128>(to_type))
        return monotonicityForType(type);
    if (const auto * type = checkAndGetDataType<DataTypeInt256>(to_type))
        return monotonicityForType(type);
    if (const auto * type = checkAndGetDataType<DataTypeFloat32>(to_type))
        return monotonicityForType(type);
    if (const auto * type = checkAndGetDataType<DataTypeFloat64>(to_type))
        return monotonicityForType(type);
    if (const auto * type = checkAndGetDataType<DataTypeDate>(to_type))
        return monotonicityForType(type);
    if (const auto * type = checkAndGetDataType<DataTypeDate32>(to_type))
        return monotonicityForType(type);
    if (const auto * type = checkAndGetDataType<DataTypeDateTime>(to_type))
        return monotonicityForType(type);
    if (const auto * type = checkAndGetDataType<DataTypeString>(to_type))
        return monotonicityForType(type);
    if (isEnum(from_type))
    {
        if (const auto * type = checkAndGetDataType<DataTypeEnum8>(to_type))
            return monotonicityForType(type);
        if (const auto * type = checkAndGetDataType<DataTypeEnum16>(to_type))
            return monotonicityForType(type);
    }
    /// other types like Null, FixedString, Array and Tuple have no monotonicity defined
    return {};
}

}
