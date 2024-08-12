#include <Processors/Formats/PullingOutputFormat.h>
#include <IO/WriteBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

WriteBufferFromPointer PullingOutputFormat::out(nullptr, 0);

PullingOutputFormat::PullingOutputFormat(const Block & header, std::atomic_bool & consume_data_flag_)
    : IOutputFormat(header, out)
    , has_data_flag(consume_data_flag_)
{}

// has_data_flag设为true
void PullingOutputFormat::consume(Chunk chunk)
{
    if (data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PullingOutputFormat cannot consume chunk because it already has data");

    if (chunk)
        info.update(chunk.getNumRows(), chunk.allocatedBytes());

    data = std::move(chunk);

    // 在整个src folder中找'has_data_flag', 只有这里设置了has_data_flag, 
    // 看来 只有PullingOutputFormat这个processor 调用work() work()中会调用consume后，has_data_flag才为true，PullingPipelineExecutor的executeStep才返回
    has_data_flag = true;
}

// has_data_flag设为false
//
Chunk PullingOutputFormat::getChunk()
{
    auto chunk = std::move(data);
    has_data_flag = false;
    return chunk;
}

Chunk PullingOutputFormat::getTotals() { return std::move(totals); }
Chunk PullingOutputFormat::getExtremes() { return std::move(extremes); }

void PullingOutputFormat::setRowsBeforeLimit(size_t rows_before_limit)
{
    info.setRowsBeforeLimit(rows_before_limit);
}
void PullingOutputFormat::setRowsBeforeAggregation(size_t rows_before_aggregation)
{
    info.setRowsBeforeAggregation(rows_before_aggregation);
}
}
