#include <Processors/Merges/IMergingTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

IMergingTransformBase::IMergingTransformBase(
    size_t num_inputs,
    const Block & input_header,
    const Block & output_header,
    bool have_all_inputs_,
    UInt64 limit_hint_,
    bool always_read_till_end_)
    : IProcessor(InputPorts(num_inputs, input_header), {output_header})
    , have_all_inputs(have_all_inputs_)
    , limit_hint(limit_hint_)
    , always_read_till_end(always_read_till_end_)
{
}

static InputPorts createPorts(const Blocks & blocks)
{
    InputPorts ports;
    for (const auto & block : blocks)
        ports.emplace_back(block);
    return ports;
}

IMergingTransformBase::IMergingTransformBase(
    const Blocks & input_headers,
    const Block & output_header,
    bool have_all_inputs_,
    UInt64 limit_hint_,
    bool always_read_till_end_)
    : IProcessor(createPorts(input_headers), {output_header})
    , have_all_inputs(have_all_inputs_)
    , limit_hint(limit_hint_)
    , always_read_till_end(always_read_till_end_)
{
}

void IMergingTransformBase::onNewInput()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "onNewInput is not implemented for {}", getName());
}

// 简单merge场景，似乎不会调这个。因为have_all_inputs构造时为true

// IMergingTransformBase::onNewInput未实现，看其子类是否实现
void IMergingTransformBase::addInput()
{
    if (have_all_inputs)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "IMergingTransform already have all inputs.");

    inputs.emplace_back(outputs.front().getHeader(), this);
    onNewInput();
}

void IMergingTransformBase::setHaveAllInputs()
{
    if (have_all_inputs)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "IMergingTransform already have all inputs.");

    have_all_inputs = true;
}

/*
inputs为 InputPorts类型
IMergingTransformBase : IProcessor 
InputPorts inputs是IProcessor的成员变量

vector<InputState> input_states
InputState = InputPort + is_initialized

state: 
*/
// 被prepare调用
IProcessor::Status IMergingTransformBase::prepareInitializeInputs()
{
    /// Add information about inputs.
    // input_states: vector<InputState>
    if (input_states.empty())
    {
        input_states.reserve(inputs.size());
        for (auto & input : inputs)
            input_states.emplace_back(input);
        
        // 之后会设置state.init_chunks
        state.init_chunks.resize(inputs.size());
    

    /// Check for inputs we need.
    bool all_inputs_has_data = true;
    auto it = inputs.begin();
    for (size_t i = 0; it != inputs.end(); ++i, ++it)
    {
        auto & input = *it;
        if (input.isFinished())
            continue;

        if (input_states[i].is_initialized)
            continue;

        input.setNeeded();

        if (!input.hasData())
        {
            all_inputs_has_data = false;
            continue;
        }

        /// setNotNeeded after reading first chunk, because in optimismtic case
        /// (e.g. with optimized 'ORDER BY primary_key LIMIT n' and small 'n')
        /// we won't have to read any chunks anymore;
        // 。。。之后可看
        auto chunk = input.pull(limit_hint != 0);
        if ((limit_hint && chunk.getNumRows() < limit_hint) || always_read_till_end)
            input.setNeeded();

        if (!chunk.hasRows())
        {
            if (!input.isFinished())
            {
                input.setNeeded();
                all_inputs_has_data = false;
            }

            continue;
        }

        // 设置state.init_chunks
        state.init_chunks[i].set(std::move(chunk));
        input_states[i].is_initialized = true;
    }

    if (!all_inputs_has_data)
        return Status::NeedData;

    is_initialized = true;
    return Status::Ready;
}

// 去override IProcessor的prepare()
// prepare()是被谁调的？
// next_input_to_read是如何设置的： 只会在work()中  if (status.required_source >= 0) 中被设置为 = required_source
IProcessor::Status IMergingTransformBase::prepare()
{   
    // 简单merge场景，构造时have_all_inputs为true
    if (!have_all_inputs)
        return Status::NeedData;
    
    // OutputPort类型
    auto & output = outputs.front();

    /// Special case for no inputs.
    if (inputs.empty())
    {
        output.finish();
        onFinish();
        return Status::Finished;
    }

    /// Check can output.

    if (output.isFinished())
    {
        for (auto & in : inputs)
            in.close();

        onFinish();
        return Status::Finished;
    }

    /// Do not disable inputs, so they can be executed in parallel.
    // 。。。。 
    bool is_port_full = !output.canPush();

    /// Push if has data.
    // state.output_chunk只在这里被用到 *
    // 有输出后，会output.push()      下一个processor会检查这个output? 下一个processor是谁？ 。。。
    if ((state.output_chunk || !state.output_chunk.getChunkInfos().empty()) && !is_port_full)
        output.push(std::move(state.output_chunk));         // 如果output.push()后再调output.canPush()，  是否会返回true ？

        // output.push中是否会令 is_port_full变成full？ 似乎不会？is_port_full是local变量，output.push()中access不到。
        // 那么下面返回Status::Ready？？  然后是否会继续调work() || 还是会调下游读这个port的processor的work？？？? (可能是可以的，因为不同processors的work可以in parallel执行。 但普通merge是单线程的？)
        // 如果调imergingtransform work，work又merge出一个chunk，然后又调prepare(), 可能发现port full？
       

        // port的容量是多少？ （某知乎文章说是1 chunk）

    // 第一次的时候调prepareInitializeInputs, 之后is_initialized似乎一直是true
    if (!is_initialized)
        return prepareInitializeInputs();

    if (state.is_finished)
    {
        if (is_port_full)
            return Status::PortFull;

        if (always_read_till_end)
        {
            for (auto & input : inputs)
            {
                if (!input.isFinished())
                {
                    input.setNeeded();
                    if (input.hasData())
                        std::ignore = input.pull();

                    return Status::NeedData;
                }
            }
        }

        for (auto & input : inputs)
            input.close();

        outputs.front().finish();

        onFinish();
        return Status::Finished;
    }

    // 只会读一个input的？ 似乎是的，只会从一个input port读
    // 只有在work中，status.required_source >= 0时，会设置state.need_data = true
    // 只有在work中，status.required_source >= 0时，会设置next_input_to_read
    if (state.need_data)
    {
        auto & input = input_states[state.next_input_to_read].port;
        if (!input.isFinished())
        {
            input.setNeeded();

            if (!input.hasData())
                return Status::NeedData;    // 来自IProcessor.h对NeedData的说明：
            // Processor needs some data at its inputs to proceed.
            // You need to run another processor to generate required input and then call 'prepare' again. 
            
            // pull
            state.input_chunk.set(input.pull());
            if (!state.input_chunk.chunk.hasRows() && !input.isFinished())
                return Status::NeedData;

            state.has_input = true;
        }
        else
        {
            state.no_data = true;
        }

        state.need_data = false;
    }

    if (is_port_full)
        return Status::PortFull;

    // 如果next_input_to_read这个input isFinished(), 也是返回Status::Ready

    return Status::Ready;
}

}
