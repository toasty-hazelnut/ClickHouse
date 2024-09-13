#pragma once

#include <IO/ReadBuffer.h>
#include <Common/PODArray.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>

namespace Poco { class Logger; }


namespace DB
{

// 
/// Tiny struct, stores number of a Part from which current row was fetched, and insertion flag.
// 。。。。MASK_FLAG 相关暂时忽略了，似乎是和skip flag相关的 
struct RowSourcePart
{
    UInt8 data = 0;

    RowSourcePart() = default;

    explicit RowSourcePart(size_t source_num, bool skip_flag = false)
    {
        static_assert(sizeof(*this) == 1, "Size of RowSourcePart is too big due to compiler settings");
        setSourceNum(source_num);
        setSkipFlag(skip_flag);
    }


    // 返回data的低7位
    size_t getSourceNum() const { return data & MASK_NUMBER; }

    /// In CollapsingMergeTree case flag means "skip this rows"
    bool getSkipFlag() const { return (data & MASK_FLAG) != 0; }

    
    void setSourceNum(size_t source_num)
    {
        data = (data & MASK_FLAG) | (static_cast<UInt8>(source_num) & MASK_NUMBER);
    }

    void setSkipFlag(bool flag)
    {
        data = flag ? data | MASK_FLAG : data & ~MASK_FLAG;
    }

    static constexpr size_t MAX_PARTS = 0x7F;
    static constexpr UInt8 MASK_NUMBER = 0x7F;
    static constexpr UInt8 MASK_FLAG = 0x80;
};

// （这个全局搜索都没有用到过
using MergedRowSources = PODArray<RowSourcePart>;


/** Gather single stream from multiple streams according to streams mask.
  * Stream mask maps row number to index of source stream.
  * Streams should contain exactly one column.
  */
  // multiple streams ----according to streams mask---> single stream
  // streams只含一个column
class ColumnGathererStream final : public IMergingAlgorithm
{
public:
    ColumnGathererStream(
        size_t num_inputs,
        ReadBuffer & row_sources_buf_,
        size_t block_preferred_size_rows_,
        size_t block_preferred_size_bytes_,
        bool is_result_sparse_);

    const char * getName() const override { return "ColumnGathererStream"; }
    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

    /// for use in implementations of IColumn::gather()
    /* 见IColumn.cpp中的  
    template <typename Derived, typename Parent>
    void IColumnHelper<Derived, Parent>::gather(ColumnGathererStream & gatherer)   // 这个override了IColumn::gather
    {    // 在这个implementation中调了gatherer.gather()
*/
    template <typename Column>
    void gather(Column & column_res);

    UInt64 getMergedRows() const { return merged_rows; }
    UInt64 getMergedBytes() const { return merged_bytes; }

private:
    /// Cache required fields   // 为何说是cache?

    // ColumnPtr 是只有一个block，还是很多个block？？可能只是一个block?因为下面有udpate?
    struct Source
    {
        ColumnPtr column;
        size_t pos = 0;
        size_t size = 0;

        void update(ColumnPtr column_)
        {
            column = std::move(column_);
            size = column->size();
            pos = 0;
        }
    };

    MutableColumnPtr result_column;

    std::vector<Source> sources;
    ReadBuffer & row_sources_buf;   // 是从文件读，eof()就读完了
    //  

    const size_t block_preferred_size_rows;
    const size_t block_preferred_size_bytes;
    const bool is_result_sparse;

    Source * source_to_fully_copy = nullptr;

    ssize_t next_required_source = -1;
    UInt64 merged_rows = 0;
    UInt64 merged_bytes = 0;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// vertical阶段，一个ColumnGathererTransform能处理几个列？ 似乎是一个列。一个列一个列merge的，见void MergeTask::VerticalMergeStage::prepareVerticalMergeForOneColumn(
class ColumnGathererTransform final : public IMergingTransform<ColumnGathererStream>
{
public:
    ColumnGathererTransform(
        const Block & header,
        size_t num_inputs,
        ReadBuffer & row_sources_buf_,
        size_t block_preferred_size_rows_,
        size_t block_preferred_size_bytes_,
        bool is_result_sparse_);

    String getName() const override { return "ColumnGathererTransform"; }

    void work() override;

protected:
    void onFinish() override;
    UInt64 elapsed_ns = 0;

    LoggerPtr log;
};
//   IMergingTransform<ColumnGathererStream> 有一个 ColumnGathererStream algorithm;成员
//   则ColumnGathererTransform也有一个 ColumnGathererStream algorithm;成员

// 如果每列一个ColumnGathererTransform的话，row_sources_buf_ 是ReadBuffer& ，是否引用的是同一个ReadBuffer？ 如果是， 这个ReadBuffer的重置在哪？。。 在MergeTask.cpp中，有调seek



// ColumnGathererStream::merge(){   ... gather() ... }
// column_res会是 ColumnGathererStream中的 result_column成员
template <typename Column>
void ColumnGathererStream::gather(Column & column_res)
{
    row_sources_buf.nextIfAtEnd();   
    RowSourcePart * row_source_pos = reinterpret_cast<RowSourcePart *>(row_sources_buf.position());
    RowSourcePart * row_sources_end = reinterpret_cast<RowSourcePart *>(row_sources_buf.buffer().end());

    // 例如，上次是因为获得出了一个chunk 而merge返回，这样的话 会把整个result_column中的内容放到结果中。于是start new column, 这里的column_res是新的
    // 
    if (next_required_source == -1)
    {
        /// Start new column.
        /// Actually reserve works only for fixed size columns.
        /// So it's safe to ignore preferred size in bytes and call reserve for number of rows.
        size_t size_to_reserve = std::min(static_cast<size_t>(row_sources_end - row_source_pos), block_preferred_size_rows);
        column_res.reserve(size_to_reserve);
    }

    // 如果之前的merge()的gather()调用中设置了next_required_source, 再次调用merge()的gather()时，应该已经准备好了。所以重置为-1 
    next_required_source = -1;

    /// We use do ... while here to ensure there will be at least one iteration of this loop.
    /// Because the column_res.byteSize() could be bigger than block_preferred_size_bytes already at this point.
    /*
    - 为何可能already bigger?？？？？？？  感觉不可能bigger??（如果bigger，一定是有从非bigger ->变成bigger的过程


    设gather()调用2时，do{}刚开始时bigger，则gather()的上一次调用 称为调用1 有几种可能：
    1）由row_source_pos >= row_sources_end返回
    则返回时，< block_preferred_size_rows, gather调用2开始时依旧 < 
    2）由next_required_source返回, 
    则返回时，< block_preferred_size_rows, gather调用2开始时依旧 < 
    3）由column_res.size() >= block_preferred_size_rows返回，设返回时 > , 
    则一定没有设置next_required_source，没有设置source_to_fully_copy （如果设置了，则不会等到 column_res.size() >= block_preferred_size_rows 才返回了）
    则在merge()中，会清空res_column。
    则gather调用2开始时，res_column是空的

    - 以及为何already bigger的话，就需要do while? 因为如果already bigger 且用 while()的话，就相当于这次不会进行循环。如果不进行循环会怎样？？

    */ 
    do
    {
        if (row_source_pos >= row_sources_end)   // 何时会出现这种？
            break;

        RowSourcePart row_source = *row_source_pos;
        size_t source_num = row_source.getSourceNum();
        Source & source = sources[source_num];
        bool source_skip = row_source.getSkipFlag();

        if (source.pos >= source.size) /// Fetch new block from source_num part
        {
            next_required_source = source_num;
            return;
        }

        ++row_source_pos;

        /// Consecutive optimization. TODO: precompute lengths
        // 或许可看下这个TODO 。。。（以及可能的其他TODO
        size_t len = 1;
        // 为何会出现 前者小于后者的情况？ （猜测，比如这个row source buffer不够了
        size_t max_len = std::min(static_cast<size_t>(row_sources_end - row_source_pos), source.size - source.pos); // interval should be in the same block （这个注释似乎是原有的

        while (len < max_len && row_source_pos->data == row_source.data)
        {
            ++len;
            ++row_source_pos;
        }

        row_sources_buf.position() = reinterpret_cast<char *>(row_source_pos);

        if (!source_skip)
        {
            /// Whole block could be produced via copying pointer from current block
            if (source.pos == 0 && source.size == len)
            {
                /// If current block already contains data, return it.
                /// Whole column from current source will be returned on next read() iteration.
                // 'via copying pointer from current block'? 
                source_to_fully_copy = &source;         // Source*  source_to_fully_copy只会在这里被设置
                return;
            }
            else if (len == 1)
                column_res.insertFrom(*source.column, source.pos);    
            else
                column_res.insertRangeFrom(*source.column, source.pos, len);  
        }

        source.pos += len;
    } while (column_res.size() < block_preferred_size_rows && column_res.byteSize() < block_preferred_size_bytes);
}

}

/* 
row source buffer相关

BufferBase.h中  
    /// Cursor in the buffer. The position of write or read. 
    using Position = char *;
    
     /// get (for reading and modifying) the position in the buffer
    Position & position() { return pos; }


如果row source buffer耗尽了，猜测是通过row_sources_buf.nextIfAtEnd();  获取新的buffer？
似乎的过程是：
从gather()返回后，merge中会把当前res作为一个chunk返回。
之后再调merge() -> gather(), gather()中就会row_sources_buf.nextIfAtEnd();   从而推进buffer


*/

/*
do while过程：

看row_source_pos有多少连续相同的(即来自同一个source), 有len个连续相同的。   (<- 这里可能的优化，precompute lengths。 见下方 )
把len插入column_res中。
    特殊情况： source_to_fully_copy: source.pos初始值为0,且整个source block都  ‘Whole block could be produced via copying pointer from current block’
row_source_pos往后移动len个

* 直到要插入的source耗尽了 / row_sources_end了 / source_to_fully_copy / column_res.size() >= block_preferred_size_rows



对比下source_to_fully_copy copy by pointer 是否比 column_res.insertRangeFrom() 更快？。。。
以及insertFrom vs. insertRangeFrom  。。。   二者都是有调 memcpy，构造出一个column，然后Chunk res.addColumn(columnptr)?
而source_to_fully是，Chunk res.addColumn(sourcecolumnptr)？不用现构造出一个column？ 

*/

/*
关于precompute length:
是否是对每列都重新计算？如果是的话，感觉这个优化还可以。 因为要gather的多列的row source是一样的，如果对每列都重新计算，那优化可以）
*/