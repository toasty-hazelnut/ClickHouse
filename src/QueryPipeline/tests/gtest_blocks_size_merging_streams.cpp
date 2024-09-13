#include <gtest/gtest.h>
#include <Core/Block.h>
#include <Columns/ColumnVector.h>
#include <Processors/Sources/BlocksListSource.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>

using namespace DB;

static Block getBlockWithSize(const std::vector<std::string> & columns, size_t rows, size_t stride, size_t & start)
{

    ColumnsWithTypeAndName cols;
    size_t size_of_row_in_bytes = columns.size() * sizeof(UInt64);

    // 依次生成各column
    for (size_t i = 0; i * sizeof(UInt64) < size_of_row_in_bytes; ++i)
    {
        auto column = ColumnUInt64::create(rows, 0);
        for (size_t j = 0; j < rows; ++j)
        {
            column->getElement(j) = start;
            start += stride;
        }
        cols.emplace_back(std::move(column), std::make_shared<DataTypeUInt64>(), columns[i]);  // column, type, column name
    }
    return Block(cols);
}


static Pipe getInputStreams(const std::vector<std::string> & column_names, const std::vector<std::tuple<size_t, size_t, size_t>> & block_sizes)
{
    Pipes pipes;

    // 每次生成一个pipe，一个pipe中的source是 BlocksListSource(BlocksList)
    for (auto [block_size_in_bytes, blocks_count, stride] : block_sizes)
    {
        BlocksList blocks;
        size_t start = stride;
        while (blocks_count--)
            blocks.push_back(getBlockWithSize(column_names, block_size_in_bytes, stride, start));
        pipes.emplace_back(std::make_shared<BlocksListSource>(std::move(blocks)));
    }
    return Pipe::unitePipes(std::move(pipes));

}


static Pipe getInputStreamsEqualStride(const std::vector<std::string> & column_names, const std::vector<std::tuple<size_t, size_t, size_t>> & block_sizes)
{
    Pipes pipes;
    size_t i = 0;
    for (auto [block_size_in_bytes, blocks_count, stride] : block_sizes)
    {
        BlocksList blocks;
        size_t start = i;
        while (blocks_count--)
            blocks.push_back(getBlockWithSize(column_names, block_size_in_bytes, stride, start));
        pipes.emplace_back(std::make_shared<BlocksListSource>(std::move(blocks)));
        i++;
    }
    return Pipe::unitePipes(std::move(pipes));

}


static SortDescription getSortDescription(const std::vector<std::string> & column_names)
{
    SortDescription descr;
    for (const auto & column : column_names)
    {
        descr.emplace_back(column, 1, 1);
    }
    return descr;
}

/*
BlocksListSource1:
1block 5rows, K1: 1 2 3 4 5, K2: 6 7 8 9 10, K3: 

BlocksListSource2:
1block 10rows, K1: 2 4 6 8 10 12 14 16 18 20, K2: 22 ..., K3: ... 

BlocksListSource3:
1block 21rows, K1: 3 6 9 12 15 18 21 24 ..., K2: ..., K3: 
*/ 
TEST(MergingSortedTest, SimpleBlockSizeTest)
{
    std::vector<std::string> key_columns{"K1", "K2", "K3"};
    auto sort_description = getSortDescription(key_columns);
    auto pipe = getInputStreams(key_columns, {{5, 1, 1}, {10, 1, 2}, {21, 1, 3}});

    EXPECT_EQ(pipe.numOutputPorts(), 3);
    
    /* 构造函数参数：   const Block & header,
    size_t num_inputs,
    const SortDescription & description_,
    size_t max_block_size_rows,
    size_t max_block_size_bytes,
    SortingQueueStrategy sorting_queue_strategy,
    UInt64 limit_,
    bool always_read_till_end_,
    WriteBuffer * out_row_sources_buf_,
    bool quiet_,
    bool use_average_block_sizes,
    bool have_all_inputs_
    */
    auto transform = std::make_shared<MergingSortedTransform>(pipe.getHeader(), pipe.numOutputPorts(), sort_description,
        8192, /*max_block_size_bytes=*/0, SortingQueueStrategy::Batch, 0, false, nullptr, false, true);

    pipe.addTransform(std::move(transform));

    QueryPipeline pipeline(std::move(pipe));
    PullingPipelineExecutor executor(pipeline);

    size_t total_rows = 0;
    Block block1;
    Block block2;

    // 
    executor.pull(block1);
    executor.pull(block2);

    Block tmp_block;
    ASSERT_FALSE(executor.pull(tmp_block));

    for (const auto & block : {block1, block2})
        total_rows += block.rows();
    
    // 为何结果是2 blocks这样:
    /*
    ????
    似乎和 use_average_block_sizes = true有关，影响hasEnoughRows()

    */

    /**
      * First block consists of 1 row from block3 with 21 rows + 2 rows from block2 with 10 rows
      * + 5 rows from block 1 with 5 rows granularity
      */
    EXPECT_EQ(block1.rows(), 8);
    /**
      * Second block consists of 8 rows from block2 + 20 rows from block3
      */
    EXPECT_EQ(block2.rows(), 28);

    EXPECT_EQ(total_rows, 5 + 10 + 21);
}

/*
BlocksListSource1:
1block 1000 rows, K1: 0 3 6 9 , K2: ..., K3: 

BlocksListSource2:
1block 1500 rows, K1: 1 4 7 10 , K2: 22 ..., K3: ... 

BlocksListSource3:
1block 1400 rows, K1: 2 5 8 11 ..., K2: ..., K3: 
似乎是这样，待验证。。
*/ 
TEST(MergingSortedTest, MoreInterestingBlockSizes)
{
    std::vector<std::string> key_columns{"K1", "K2", "K3"};
    auto sort_description = getSortDescription(key_columns);
    auto pipe = getInputStreamsEqualStride(key_columns, {{1000, 1, 3}, {1500, 1, 3}, {1400, 1, 3}});

    EXPECT_EQ(pipe.numOutputPorts(), 3);

    auto transform = std::make_shared<MergingSortedTransform>(pipe.getHeader(), pipe.numOutputPorts(), sort_description,
        8192, /*max_block_size_bytes=*/0, SortingQueueStrategy::Batch, 0, false, nullptr, false, true);

    pipe.addTransform(std::move(transform));

    QueryPipeline pipeline(std::move(pipe));
    PullingPipelineExecutor executor(pipeline);

    Block block1;
    Block block2;
    Block block3;
    executor.pull(block1);
    executor.pull(block2);
    executor.pull(block3);

    Block tmp_block;
    ASSERT_FALSE(executor.pull(tmp_block));

    EXPECT_EQ(block1.rows(), (1000 + 1500 + 1400) / 3);
    EXPECT_EQ(block2.rows(), (1000 + 1500 + 1400) / 3);
    EXPECT_EQ(block3.rows(), (1000 + 1500 + 1400) / 3);

    EXPECT_EQ(block1.rows() + block2.rows() + block3.rows(), 1000 + 1500 + 1400);
}

/*
对于test2:
每次都是依次从src1, src2, src3取。
于是每个merged row的所在的block size依次为:1000, 1500, 1400, 1000, 1500, 1400 ...
avg约为1300
所以当merged_rows >= avg时,就会hasEnoughRows, 从而输出一个block

以上为粗略猜测，还待捋是不是如此。。

*/