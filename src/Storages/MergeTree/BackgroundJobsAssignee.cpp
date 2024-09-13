#include <Storages/MergeTree/BackgroundJobsAssignee.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/CurrentMetrics.h>
#include <Common/randomSeed.h>
#include <Interpreters/Context.h>
#include <pcg_random.hpp>
#include <random>

namespace DB
{

BackgroundJobsAssignee::BackgroundJobsAssignee(MergeTreeData & data_, BackgroundJobsAssignee::Type type_, ContextPtr global_context_)
    : WithContext(global_context_)
    , data(data_)
    , sleep_settings(global_context_->getBackgroundMoveTaskSchedulingSettings())
    , rng(randomSeed())
    , type(type_)
{
}

// scheduleMergeMutateTask中调用trigger时，holder要holder的应该是？？？  
// 然后holder->schedule，会把这个merge task加入到bgSchedulePool的tasks queue中，于是worker threads会从queue中取task 并执行。
// 这个
void BackgroundJobsAssignee::trigger()
{
    std::lock_guard lock(holder_mutex);

    if (!holder)
        return;

    /// Do not reset backoff factor if some task has appeared,
    /// but decrease it exponentially on every new task.
    no_work_done_count /= 2;
    /// We have background jobs, schedule task as soon as possible
    holder->schedule();
}

void BackgroundJobsAssignee::postpone()
{
    std::lock_guard lock(holder_mutex);

    if (!holder)
        return;

    no_work_done_count += 1;
    double random_addition = std::uniform_real_distribution<double>(0, sleep_settings.task_sleep_seconds_when_no_work_random_part)(rng);

    size_t next_time_to_execute = static_cast<size_t>(
        1000 * (std::min(
            sleep_settings.task_sleep_seconds_when_no_work_max,
            sleep_settings.thread_sleep_seconds_if_nothing_to_do * std::pow(sleep_settings.task_sleep_seconds_when_no_work_multiplier, no_work_done_count))
        + random_addition));

    holder->scheduleAfter(next_time_to_execute, false);
}

// 在哪里被调？
// 返回是否scheduled
bool BackgroundJobsAssignee::scheduleMergeMutateTask(ExecutableTaskPtr merge_task)
{   
    // getMergeMutateExecutor() 返回 shared_ptr<MergeMutateBackgroundExecutor>
    // "using MergeMutateBackgroundExecutor = MergeTreeBackgroundExecutor<DynamicRuntimeQueue>;"
    bool res = getContext()->getMergeMutateExecutor()->trySchedule(merge_task);

    // 加入到MergeTreeBackgroundExecutor的pending queue中，为何这里还要调trigger()??
    res ? trigger() : postpone();
    return res;
}


bool BackgroundJobsAssignee::scheduleFetchTask(ExecutableTaskPtr fetch_task)
{
    bool res = getContext()->getFetchesExecutor()->trySchedule(fetch_task);
    res ? trigger() : postpone();
    return res;
}


bool BackgroundJobsAssignee::scheduleMoveTask(ExecutableTaskPtr move_task)
{
    bool res = getContext()->getMovesExecutor()->trySchedule(move_task);
    res ? trigger() : postpone();
    return res;
}


bool BackgroundJobsAssignee::scheduleCommonTask(ExecutableTaskPtr common_task, bool need_trigger)
{
    bool schedule_res = getContext()->getCommonExecutor()->trySchedule(common_task);
    schedule_res && need_trigger ? trigger() : postpone();
    return schedule_res;
}


String BackgroundJobsAssignee::toString(Type type)
{
    switch (type)
    {
        case Type::DataProcessing:
            return "DataProcessing";
        case Type::Moving:
            return "Moving";
    }
}

// start中设置了holder
void BackgroundJobsAssignee::start()
{
    std::lock_guard lock(holder_mutex);
    if (!holder)
        holder = getContext()->getSchedulePool().createTask("BackgroundJobsAssignee:" + toString(type), [this]{ threadFunc(); });

    holder->activateAndSchedule();
}

void BackgroundJobsAssignee::finish()
{
    /// No lock here, because scheduled tasks could call trigger method
    if (holder)
    {
        holder->deactivate();

        auto storage_id = data.getStorageID();

        getContext()->getMovesExecutor()->removeTasksCorrespondingToStorage(storage_id);
        getContext()->getFetchesExecutor()->removeTasksCorrespondingToStorage(storage_id);
        getContext()->getMergeMutateExecutor()->removeTasksCorrespondingToStorage(storage_id);
        getContext()->getCommonExecutor()->removeTasksCorrespondingToStorage(storage_id);
    }
}

// 在
// (这里try catch外面没有函数体的大括号吗？)
void BackgroundJobsAssignee::threadFunc()
try
{
    bool succeed = false;
    switch (type)
    {
        case Type::DataProcessing:
            // MergeTreeData data
            succeed = data.scheduleDataProcessingJob(*this);
            break;
        case Type::Moving:
            succeed = data.scheduleDataMovingJob(*this);
            break;
    }

    if (!succeed)
        postpone();
}
catch (...) /// Catch any exception to avoid thread termination.
{
    tryLogCurrentException(__PRETTY_FUNCTION__);
    postpone();
}

BackgroundJobsAssignee::~BackgroundJobsAssignee()
{
    try
    {
        finish();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
