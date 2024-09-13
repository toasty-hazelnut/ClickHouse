#include <Processors/Merges/Algorithms/MergingSortedAlgorithm.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

MergingSortedAlgorithm::MergingSortedAlgorithm(
    Block header_,
    size_t num_inputs,
    const SortDescription & description_,
    size_t max_block_size_,
    size_t max_block_size_bytes_,
    SortingQueueStrategy sorting_queue_strategy_,
    UInt64 limit_,
    WriteBuffer * out_row_sources_buf_,
    bool use_average_block_sizes)
    : header(std::move(header_))
    , merged_data(use_average_block_sizes, max_block_size_, max_block_size_bytes_)
    , description(description_)
    , limit(limit_)
    , out_row_sources_buf(out_row_sources_buf_)
    , current_inputs(num_inputs)
    , sorting_queue_strategy(sorting_queue_strategy_)
    , cursors(num_inputs)
{
    DataTypes sort_description_types;
    sort_description_types.reserve(description.size());

    /// Replace column names in description to positions.
    // ？
    for (const auto & column_description : description)
    {
        has_collation |= column_description.collator != nullptr;
        sort_description_types.emplace_back(header.getByName(column_description.column_name).type);
    }

    queue_variants = SortQueueVariants(sort_description_types, description);
    if (queue_variants.variantSupportJITCompilation())
        compileSortDescriptionIfNeeded(description, sort_description_types, true /*increase_compile_attempts*/);
}

// 在普通merge时似乎不会被调用
void MergingSortedAlgorithm::addInput()
{
    current_inputs.emplace_back();
    cursors.emplace_back();
}

void MergingSortedAlgorithm::initialize(Inputs inputs)
{
    removeConstAndSparse(inputs);
    merged_data.initialize(header, inputs);
    current_inputs = std::move(inputs);

    // 初始化cursors
    for (size_t source_num = 0; source_num < current_inputs.size(); ++source_num)
    {
        auto & chunk = current_inputs[source_num].chunk;
        if (!chunk)
            continue;
        
        // source_num为这个cursor的order值
        cursors[source_num] = SortCursorImpl(header, chunk.getColumns(), description, source_num);
    }

    // ?语法
    if (sorting_queue_strategy == SortingQueueStrategy::Default)
    {
        queue_variants.callOnVariant([&](auto & queue)
        {
            using QueueType = std::decay_t<decltype(queue)>;
            queue = QueueType(cursors);
        });
    }
    else
    {
        queue_variants.callOnBatchVariant([&](auto & queue)
        {
            using QueueType = std::decay_t<decltype(queue)>;
            queue = QueueType(cursors);
        });
    }
}


// 猜测是某个part的一个block读完了，去读下一个block 也就是新的input
/*
 当一个block处理完，为何要 removeTop(), 把cursor从heap中移出。然后consume block时再把cursor加入heap？
heap的开销：
remove, push  vs. updateTop

cursor信息重新初始化reset的开销：
不，这些开销是必要的。没法优化掉

（是同一个curosr object.  都是cursors[source_num]

。。。或许可成为微小的优化的点？ 因为heap的开销不像一个block内curosr移动那样 次数很多。 
*/
void MergingSortedAlgorithm::consume(Input & input, size_t source_num)
{
    removeConstAndSparse(input);
    current_inputs[source_num].swap(input);
    //  Set the cursor to the beginning of the new block.
    // 会设置cursor中的all_columns为 这个chunk的columns （也就是说，cursor中的all_columns 只是指向这一个block的clolumns。 这个block的columns耗尽后，就会clear cusror的很多信息，重新加载
    cursors[source_num].reset(current_inputs[source_num].chunk.getColumns(), header);

    if (sorting_queue_strategy == SortingQueueStrategy::Default)
    {
        queue_variants.callOnVariant([&](auto & queue)
        {   
            // 会把这个cursor重新
            queue.push(cursors[source_num]);
        });
    }
    else
    {
        queue_variants.callOnBatchVariant([&](auto & queue)
        {
            queue.push(cursors[source_num]);
        });
    }
}

// queue指？？？ 其类型是？
// 似乎是SortingQueueImpl<Cursor, SortingQueueStrategy::Default>;  见SortCursor.h

IMergingAlgorithm::Status MergingSortedAlgorithm::merge()
{
    if (sorting_queue_strategy == SortingQueueStrategy::Default)
    {   // 普通merge用的是Default
        IMergingAlgorithm::Status result = queue_variants.callOnVariant([&](auto & queue)
        {
            return mergeImpl(queue);
        });

        return result;
    }

    IMergingAlgorithm::Status result = queue_variants.callOnBatchVariant([&](auto & queue)
    {
        return mergeBatchImpl(queue);
    });

    return result;
}

/*
哪些情况merge()会返回：
merge出了一个chunk；
队首的chunk耗尽了.

但队首chunk单纯耗尽（不是totallylessthan时有mergedRows）的情况中：
merge()并不会返回含chunk的Status.
于是MergeTask会继续进行、整个pipeline会继续执行，会让input source work, ... 再次调用work -> merge, 直至mergeImpl返回了含chunk的Status

*/  
template <typename TSortingHeap>
IMergingAlgorithm::Status MergingSortedAlgorithm::mergeImpl(TSortingHeap & queue)
{
    /// Take rows in required order and put them into `merged_data`, while the rows are no more than `max_block_size`
    while (queue.isValid())
    {
        if (merged_data.hasEnoughRows())
            return Status(merged_data.pull());      // Status(Chunk chunk);
        
        // queue.current()就是队首的
        auto current = queue.current();

        if (current.impl->isLast() && current_inputs[current.impl->order].skip_last_row)
        {
            /// Get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);     // Status(required_source)  
        }

        // queue.nextChild()是较小的那个child
        if (current.impl->isFirst()
            && !current_inputs[current.impl->order].skip_last_row /// Ignore optimization if last row should be skipped.
            && (queue.size() == 1
                || (queue.size() >= 2 && current.totallyLessOrEquals(queue.nextChild()))))
        {
            /** This is special optimization if current cursor is totally less than next cursor.
              * We want to insert current cursor chunk directly in merged data.
              *
              * First if merged_data is not empty we need to flush it.
              * We will get into the same condition on next mergeBatch call.   是的
              *
              * Then we can insert chunk directly in merged data.
              */
            // current.impl->isFirst时，是有可能出现merged_data非空的 （例如，上一次因为队首chunk耗尽了 于是merge结束运行了 但只是reutrn Status(required_source)，不会return Status(chunk, required_source)）
            // 为何要先flush it？？ 是没有空间放多于一个chunk吗还是？
            if (merged_data.mergedRows() != 0)
                return Status(merged_data.pull());      // Status(chunk)

            /// Actually, current.impl->order stores source number (i.e. cursors[current.impl->order] == current.impl)
            size_t source_num = current.impl->order;

            auto chunk_num_rows = current_inputs[source_num].chunk.getNumRows();

            // 按上面的逻辑 if(merged_data.mergedRows() != 0), 这里mergedRows一定等于0
            // 所以只是check 这个chunk 是否大于limit？
            // 但为何变量名叫total_merged_rows_after_insertion? 而且下面根据limit_reached设置 status中的is_finished？？？
            UInt64 total_merged_rows_after_insertion = merged_data.mergedRows() + chunk_num_rows;
            bool limit_reached = limit && total_merged_rows_after_insertion >= limit;

            if (limit && total_merged_rows_after_insertion > limit)
                chunk_num_rows -= total_merged_rows_after_insertion - limit;

            // 插入chunk_num_rows这么多行。 先整个'插入'，再pop
            // 如果超过limit，超过limit的部分似乎就直接丢弃了
            merged_data.insertChunk(std::move(current_inputs[source_num].chunk), chunk_num_rows);   //  内部: columns[i] = std::move(chunk_columns[i]);
            current_inputs[source_num].chunk = Chunk();

            /// Write order of rows for other columns this data will be used in gather stream
            if (out_row_sources_buf)
            {
                RowSourcePart row_source(source_num);
                for (size_t i = 0; i < chunk_num_rows; ++i)
                    // 写同样的row_source.data
                    out_row_sources_buf->write(row_source.data);
            }

            /// We will get the next block from the corresponding source, if there is one.
            // 下面设置了required_source
            queue.removeTop();

            auto status = Status(merged_data.pull(), limit_reached);

            if (!limit_reached)
                status.required_source = source_num;

            return status;
        }

        // 插入single row
        // all_columns？ 似乎同input.chunk.getColumns  
        // 内部会调Column的insertFrom，同vertical中的insertFrom （但因为horizontal中是插入多个列，所以用merged_data包了一层）
        merged_data.insertRow(current->all_columns, current->getRow(), current->rows);  // all_columns （类型为ColumnRawPtrs），pos，一共多少rows

        if (out_row_sources_buf)
        {
            RowSourcePart row_source(current.impl->order);
            out_row_sources_buf->write(row_source.data);
        }

        if (limit && merged_data.totalMergedRows() >= limit)
            return Status(merged_data.pull(), true);

        if (!current->isLast())
        {
            queue.next();
        }
        else
        {
            /// We will get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);     // 此时没有chunk，只是required_source
        }
    }

    return Status(merged_data.pull(), true);
}


template <typename TSortingQueue>
IMergingAlgorithm::Status MergingSortedAlgorithm::mergeBatchImpl(TSortingQueue & queue)
{
    /// Take rows in required order and put them into `merged_data`, while the rows are no more than `max_block_size`
    while (queue.isValid())
    {
        if (merged_data.hasEnoughRows())
            return Status(merged_data.pull());

        auto [current_ptr, initial_batch_size] = queue.current();
        auto current = *current_ptr;

        bool batch_skip_last_row = false;
        if (current.impl->isLast(initial_batch_size) && current_inputs[current.impl->order].skip_last_row)
        {
            batch_skip_last_row = true;

            if (initial_batch_size == 1)
            {
                /// Get the next block from the corresponding source, if there is one.
                queue.removeTop();
                return Status(current.impl->order);
            }
        }

        UInt64 merged_rows = merged_data.mergedRows();
        size_t updated_batch_size = initial_batch_size;

        if (merged_rows + updated_batch_size > merged_data.maxBlockSize())
        {
            batch_skip_last_row = false;
            updated_batch_size -= merged_rows + updated_batch_size - merged_data.maxBlockSize();
        }

        bool limit_reached = false;

        if (limit && merged_rows + updated_batch_size >= limit && !batch_skip_last_row)
        {
            updated_batch_size -= merged_rows + updated_batch_size - limit;
            limit_reached = true;
        }
        else if (limit && merged_rows + updated_batch_size > limit)
        {
            batch_skip_last_row = false;
            updated_batch_size -= merged_rows + updated_batch_size - limit;
            limit_reached = true;
        }

        if (unlikely(current.impl->isFirst() &&
            current.impl->isLast(initial_batch_size) &&
            !current_inputs[current.impl->order].skip_last_row))
        {
            /** This is special optimization if current cursor is totally less than next cursor.
              * We want to insert current cursor chunk directly in merged data.
              *
              * First if merged_data is not empty we need to flush it.
              * We will get into the same condition on next mergeBatch call.
              *
              * Then we can insert chunk directly in merged data.
              */
            if (merged_data.mergedRows() != 0)
                return Status(merged_data.pull());

            size_t source_num = current.impl->order;
            size_t insert_rows_size = initial_batch_size - static_cast<size_t>(batch_skip_last_row);
            merged_data.insertChunk(std::move(current_inputs[source_num].chunk), insert_rows_size);
            current_inputs[source_num].chunk = Chunk();

            if (out_row_sources_buf)
            {
                RowSourcePart row_source(current.impl->order);

                for (size_t i = 0; i < insert_rows_size; ++i)
                    out_row_sources_buf->write(row_source.data);
            }

            /// We will get the next block from the corresponding source, if there is one.
            queue.removeTop();

            auto result = Status(merged_data.pull(), limit_reached);
            if (!limit_reached)
                result.required_source = source_num;

            return result;
        }

        size_t insert_rows_size = updated_batch_size - static_cast<size_t>(batch_skip_last_row);
        merged_data.insertRows(current->all_columns, current->getRow(), insert_rows_size, current->rows); // 内部会调Column的insertRangeFrom

        if (out_row_sources_buf)
        {
            RowSourcePart row_source(current.impl->order);

            for (size_t i = 0; i < insert_rows_size; ++i)
                out_row_sources_buf->write(row_source.data);
        }

        if (limit_reached)
            break;

        if (!current->isLast(updated_batch_size))
        {
            queue.next(updated_batch_size);  // 
        }
        else
        {
            /// We will get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);
        }
    }

    return Status(merged_data.pull(), true);
}

}
