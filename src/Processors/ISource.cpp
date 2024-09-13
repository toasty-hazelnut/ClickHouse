#include <Processors/ISource.h>
#include <QueryPipeline/StreamLocalLimits.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

ISource::~ISource() = default;

ISource::ISource(Block header, bool enable_auto_progress)
    : IProcessor({}, {std::move(header)})
    , auto_progress(enable_auto_progress)
    , output(outputs.front())
{
}

/*
work() { has_input = true; current_chunk = ; } -> prepare() { output.pushData(); has_input = false; return Status::PortFull; }
下游processor调prepare(){  }
prepare(){ output.canPush(); return Ready; }

work() {同上 ... }


PortFull: 来自IProcessor.h:
/// Processor cannot proceed because output port is full or not isNeeded().
/// You need to transfer data from output port (感觉指的是shared state中的data？) to the input port (感觉指的是下游processor自己成员中的data？) of another processor and then call 'prepare' again.


注，符合IProcessor.h中说的，work中不会操作port，prepare中会操作port

*/
ISource::Status ISource::prepare()
{
    if (finished)
    {
        output.finish();
        return Status::Finished;
    }

    /// Check can output.
    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    if (!has_input)
        return Status::Ready;

    // 
    output.pushData(std::move(current_chunk));
    has_input = false;

    if (isCancelled())
    {
        output.finish();
        return Status::Finished;
    }

    if (got_exception)
    {
        finished = true;
        output.finish();
        return Status::Finished;
    }

    /// Now, we pushed to output, and it must be full.
    // 上面调用了output.pushData()
    return Status::PortFull;
}

void ISource::setStorageLimits(const std::shared_ptr<const StorageLimitsList> & storage_limits_)
{
    storage_limits = storage_limits_;
}

void ISource::progress(size_t read_rows, size_t read_bytes)
{
    //std::cerr << "========= Progress " << read_rows << " from " << getName() << std::endl << StackTrace().toString() << std::endl;
    read_progress_was_set = true;
    std::lock_guard lock(read_progress_mutex);
    read_progress.read_rows += read_rows;
    read_progress.read_bytes += read_bytes;
}

std::optional<ISource::ReadProgress> ISource::getReadProgress()
{
    std::lock_guard lock(read_progress_mutex);
    if (finished && read_progress.read_bytes == 0 && read_progress.total_rows_approx == 0)
        return {};

    ReadProgressCounters res_progress;
    std::swap(read_progress, res_progress);

    if (storage_limits)
        return ReadProgress{res_progress, *storage_limits};

    static StorageLimitsList empty_limits;
    return ReadProgress{res_progress, empty_limits};
}

void ISource::addTotalRowsApprox(size_t value)
{
    std::lock_guard lock(read_progress_mutex);
    read_progress.total_rows_approx += value;
}

void ISource::addTotalBytes(size_t value)
{
    std::lock_guard lock(read_progress_mutex);
    read_progress.total_bytes += value;
}

void ISource::work()
{
    try
    {
        read_progress_was_set = false;

        if (auto chunk = tryGenerate())
        {
            current_chunk.chunk = std::move(*chunk);
            if (current_chunk.chunk)
            {
                has_input = true;  //
                if (auto_progress && !read_progress_was_set)
                    progress(current_chunk.chunk.getNumRows(), current_chunk.chunk.bytes());
            }
        }
        else
            finished = true;

        if (isCancelled())
            finished = true;
    }
    catch (...)
    {
        finished = true;
        got_exception = true;
        throw;
    }
}

Chunk ISource::generate()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "generate is not implemented for {}", getName());
}

std::optional<Chunk> ISource::tryGenerate()
{
    auto chunk = generate();
    if (!chunk)
        return std::nullopt;

    return chunk;
}

}

