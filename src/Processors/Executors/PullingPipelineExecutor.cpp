#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Formats/PullingOutputFormat.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Sources/NullSource.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PullingPipelineExecutor::PullingPipelineExecutor(QueryPipeline & pipeline_) : pipeline(pipeline_)
{
    if (!pipeline.pulling())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline for PullingPipelineExecutor must be pulling");

    // PullingOutputFormat内部的 has_data_flag
    pulling_format = std::make_shared<PullingOutputFormat>(pipeline.output->getHeader(), has_data_flag);    // has_data_flag默认为false
    
    // complete中 会相当于  connect(outputPort, format->getPort(IOutputFormat::PortKind::Main);
    pipeline.complete(pulling_format);
}

PullingPipelineExecutor::~PullingPipelineExecutor()
{
    try
    {
        cancel();
    }
    catch (...)
    {
        tryLogCurrentException("PullingPipelineExecutor");
    }
}

const Block & PullingPipelineExecutor::getHeader() const
{
    return pulling_format->getPort(IOutputFormat::PortKind::Main).getHeader();
}

/// Methods return false if query is finished.
bool PullingPipelineExecutor::pull(Chunk & chunk)
{   
    // 构建executor
    if (!executor)
    {
        // 
        executor = std::make_shared<PipelineExecutor>(pipeline.processors, pipeline.process_list_element);
        executor->setReadProgressCallback(pipeline.getReadProgressCallback());
    }

    if (!executor->checkTimeLimitSoft())
        return false;
    
    // from PipelineExecutor::executeStep
    /// Execute single step. Step will be stopped when yield_flag is true.
    /// Execution is happened in a single thread.
    /// Return true if execution should be continued.

    // bool PipelineExecutor::executeStep(std::atomic_bool * yield_flag)
    // has_data_flag即为

    // 这里的executor->executeStep()应该返回true，从而pull(chunk)能pull到chunk，pull(block)返回true，
    // 从而MergeTask::ExecuteAndFinalizeHorizontalPart::execute会继续执行多次

    // 注意区分 yield_flag 和 executeStep的返回值。
    // yield_flag为true时 executeStep会返回。 但executeStep的返回值为true/false 取决于tasks.isFinished()

    // 何时返回： has_data_flag为true时，PullingOutputFormat processor调用work() 中才会consume 
    // 见PullingOutputFormat::consume
    if (!executor->executeStep(&has_data_flag))
        return false;

    //   把pulling_format中的has_data_flag设为false。 
    chunk = pulling_format->getChunk();
    return true;
}

/// Methods return false if query is finished.
bool PullingPipelineExecutor::pull(Block & block)
{
    Chunk chunk;

    if (!pull(chunk))
        return false;
    
    // pull(chunk)返回true，但chunk为空。 如果chunk为空，为何返回true？
    if (!chunk)
    {
        /// In case if timeout exceeded.
        block.clear();
        return true;
    }

    block = pulling_format->getPort(IOutputFormat::PortKind::Main).getHeader().cloneWithColumns(chunk.detachColumns());
    if (auto agg_info = chunk.getChunkInfos().get<AggregatedChunkInfo>())
    {
        block.info.bucket_num = agg_info->bucket_num;
        block.info.is_overflows = agg_info->is_overflows;
    }

    return true;
}

void PullingPipelineExecutor::cancel()
{
    /// Cancel execution if it wasn't finished.
    if (executor)
        executor->cancel();
}

Chunk PullingPipelineExecutor::getTotals()
{
    return pulling_format->getTotals();
}

Chunk PullingPipelineExecutor::getExtremes()
{
    return pulling_format->getExtremes();
}

Block PullingPipelineExecutor::getTotalsBlock()
{
    auto totals = getTotals();

    if (totals.empty())
        return {};

    const auto & header = pulling_format->getPort(IOutputFormat::PortKind::Totals).getHeader();
    return header.cloneWithColumns(totals.detachColumns());
}

Block PullingPipelineExecutor::getExtremesBlock()
{
    auto extremes = getExtremes();

    if (extremes.empty())
        return {};

    const auto & header = pulling_format->getPort(IOutputFormat::PortKind::Extremes).getHeader();
    return header.cloneWithColumns(extremes.detachColumns());
}

ProfileInfo & PullingPipelineExecutor::getProfileInfo()
{
    return pulling_format->getProfileInfo();
}

}
