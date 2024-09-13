#include <iostream>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/SimpleMergeSelector.h>


/** This program tests merge-selecting algorithm.
  * Usage:
  * ./merge_selector <<< "1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20"
  * clickhouse-client --query="SELECT 100 + round(10 * rand() / 0xFFFFFFFF) FROM system.numbers LIMIT 105" | tr '\n' ' ' | ./merge_selector
  */

 /*
 96 1 1 1 1 会merge
 97 1 1 1 1 会一直输出.
 */ 

int main(int, char **)
{
    using namespace DB;

    IMergeSelector::PartsRanges partitions(1);
    IMergeSelector::PartsRange & parts = partitions.back();
    // 所有parts 都放到 PartsRanges[0] 中，都在一个PartsRange中

    SimpleMergeSelector::Settings settings;
//    settings.base = 2;
//    settings.max_parts_to_merge_at_once = 10;
    SimpleMergeSelector selector(settings);

/*    LevelMergeSelector::Settings settings;
    settings.min_parts_to_merge = 8;
    settings.max_parts_to_merge = 16;
    LevelMergeSelector selector(settings);*/  // 没找到LevelMergeSelector这个类？

    ReadBufferFromFileDescriptor in(STDIN_FILENO);

    size_t sum_parts_size = 0;

    while (!in.eof())
    {
        size_t size = 0;
        readText(size, in);
        skipWhitespaceIfAny(in);

        IMergeSelector::Part part;
        part.size = size;     // in bytes
        part.age = 0;
        part.level = 0;
        part.data = reinterpret_cast<const void *>(parts.size());  // data的类型 类似于  DataPartPtr * 

        parts.emplace_back(part);

        sum_parts_size += size;
    }

    size_t sum_size_written = sum_parts_size;
    size_t num_merges = 1;

    while (parts.size() > 1)
    {   
        // select(PartsRanges, );
        IMergeSelector::PartsRange selected_parts = selector.select(partitions, 0);

        if (selected_parts.empty())
        {
            std::cout << '.';
            for (auto & part : parts)
                //
                ++part.age;  // part.age: how old this data part in seconds
                // 这个测试中，age只有在select没选出parts时，给所有parts age++  (这样好像不太合理？？
                // 实际中，age是怎样变的。。。？  
            continue;
        }
        std::cout << '\n';  // 如果选择了parts, 各parts的age是否会增加？

        size_t sum_merged_size = 0;
        size_t start_index = 0;
        unsigned max_level = 0;
        bool in_range = false;

        for (size_t i = 0, size = parts.size(); i < size; ++i)
        {
            if (parts[i].data == selected_parts.front().data)
            {
                std::cout << "\033[1;31m";
                in_range = true;
                start_index = i;
            }

            std::cout << parts[i].size;

            // // 由这个in_range, 可知selector.select(PartsRanges,); 会在参数的PartsRanges中的一个PartsRange中选，且选其中连续的Part
            if (in_range)   
            {
                sum_merged_size += parts[i].size;
                max_level = std::max(parts[i].level, max_level);
            }

            if (parts[i].data == selected_parts.back().data)
            {
                in_range = false;
                std::cout << "\033[0m";
            }

            std::cout << " ";
        }

        parts[start_index].size = sum_merged_size;
        parts[start_index].level = max_level + 1;
        parts[start_index].age = 0;
        parts.erase(parts.begin() + start_index + 1, parts.begin() + start_index + selected_parts.size());

        std::cout << '\n';

        sum_size_written += sum_merged_size;
        ++num_merges;
    }

    std::cout << std::fixed << std::setprecision(2)
        // write amplification: 
        << "Write amplification: " << static_cast<double>(sum_size_written) / sum_parts_size << "\n"
        << "Num merges: " << num_merges << "\n"
        << "Tree depth: " << parts.front().level << "\n"
        ;

    // 

    return 0;
}
