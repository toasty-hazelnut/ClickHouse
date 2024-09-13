#include <Storages/MergeTree/SimpleMergeSelector.h>

#include <base/interpolate.h>

#include <cmath>
#include <cassert>
#include <iostream>


namespace DB
{

namespace
{

/** Estimates best set of parts to merge within passed alternatives.
  */
struct Estimator
{
    // 
    using Iterator = SimpleMergeSelector::PartsRange::const_iterator;

    // (不含end)
    // score只和cnt, sum_size, 以及size_prev_at_left有关。 
    void consider(Iterator begin, Iterator end, size_t sum_size, size_t size_prev_at_left, const SimpleMergeSelector::Settings & settings)
    {   
        //  size_fixed_cost_to_add 5 * 1024 * 1024 
        // Add this to size before all calculations. It means: merging even very small parts has it's fixed cost.
        double current_score = score(end - begin, sum_size, settings.size_fixed_cost_to_add);

        // 。。。。
        /**
        -1/2 < log2(sum/left) < 1/2
        0.707sum < left < 1.414sum
        但因为heuristic_to_align_parts_min_ratio_of_sum_size_to_prev_part = 0.9, 所以实际上 0.9sum

        */
        if (settings.enable_heuristic_to_align_parts
            && size_prev_at_left > sum_size * settings.heuristic_to_align_parts_min_ratio_of_sum_size_to_prev_part) // 0.9
        {
            double difference = std::abs(log2(static_cast<double>(sum_size) / size_prev_at_left));
            if (difference < settings.heuristic_to_align_parts_max_absolute_difference_in_powers_of_two)   // 0.5
                current_score *= interpolateLinear(settings.heuristic_to_align_parts_max_score_adjustment /* 0.75 */, 1,
                    difference / settings.heuristic_to_align_parts_max_absolute_difference_in_powers_of_two);
        }

        // 为何要去掉右边小的。。。
        if (settings.enable_heuristic_to_remove_small_parts_at_right)
            // end>=begin+3, 当只剩2 parts时停止
            while (end >= begin + 3 && (end - 1)->size < settings.heuristic_to_remove_small_parts_at_right_max_ratio * sum_size) // 0.01
                --end;

        if (min_score == 0.0 || current_score < min_score)
        {
            min_score = current_score;
            best_begin = begin;
            best_end = end;
        }
    }

    SimpleMergeSelector::PartsRange getBest() const
    {
        return SimpleMergeSelector::PartsRange(best_begin, best_end);
    }

    static double score(double count, double sum_size, double sum_size_fixed_cost)
    {
        /** Consider we have two alternative ranges of data parts to merge.
          * Assume time to merge a range is proportional to sum size of its parts.
          *
          * Cost of query execution is proportional to total number of data parts in a moment of time.
          * Let define our target: to minimize average (in time) total number of data parts.
          *
          * Let calculate integral of total number of parts, if we are going to do merge of one or another range.
          * It must be lower, and thus we decide, what range is better to merge.
          *
          * The integral is lower iff the following formula is lower:
          *
          *  sum_size / (count - 1)
          *
          * But we have some tunes to prefer longer ranges.
          */
          /* integral指的是啥？ 
          sum_size / (count-1) 和 number of parts的关系：

          

          * tunes to prefer longer ranges指的是？
            是指相比于sum_size, count更容易减小score吗？
          */
        return (sum_size + sum_size_fixed_cost * count) / (count - 1.9);
    }

    double min_score = 0.0;
    Iterator best_begin {};
    Iterator best_end {};
};


/**
 * 1       _____
 *        /
 * 0_____/
 *      ^  ^
 *     min max
 */
double mapPiecewiseLinearToUnit(double value, double min, double max)
{
    return value <= min ? 0
        : (value >= max ? 1
        : ((value - min) / (max - min)));
}


/** Is allowed to merge parts in range with specific properties.
  */
  // allow(sum_size, max_size, min_age, end - begin, parts_count, min_size_to_lower_base_log, max_size_to_lower_base_log, settings)
// sum_size, max_size is in bytes 
bool allow(
    double sum_size,
    double max_size,
    double min_age,
    double range_size,
    double partition_size,
    double min_size_to_lower_base_log,
    double max_size_to_lower_base_log,
    const SimpleMergeSelector::Settings & settings)
{
    if (settings.min_age_to_force_merge && min_age >= settings.min_age_to_force_merge) // 默认是0
        return true;

//    std::cerr << "sum_size: " << sum_size << "\n";

    /// Map size to 0..1 using logarithmic scale
    /// Use log(1 + x) instead of log1p(x) because our sum_size is always integer.
    /// Also log1p seems to be slow and significantly affect performance of merges assignment.
    double size_normalized = mapPiecewiseLinearToUnit(log(1 + sum_size), min_size_to_lower_base_log /*ln(1+1MB)*/, max_size_to_lower_base_log /*ln(1+100GB)*/);

//    std::cerr << "size_normalized: " << size_normalized << "\n";

    /// Calculate boundaries for age
    double min_age_to_lower_base = interpolateLinear(settings.min_age_to_lower_base_at_min_size /*10*/, settings.min_age_to_lower_base_at_max_size /*10*/, size_normalized);
    double max_age_to_lower_base = interpolateLinear(settings.max_age_to_lower_base_at_min_size/*3600*/, settings.max_age_to_lower_base_at_max_size /*30*86400*/, size_normalized);
    // size_normalized越大， max_age_to_lower_base越大

//    std::cerr << "min_age_to_lower_base: " << min_age_to_lower_base << "\n";
//    std::cerr << "max_age_to_lower_base: " << max_age_to_lower_base << "\n";

    /// Map age to 0..1
    double age_normalized = mapPiecewiseLinearToUnit(min_age, min_age_to_lower_base, max_age_to_lower_base);

//    std::cerr << "age: " << min_age << "\n";
//    std::cerr << "age_normalized: " << age_normalized << "\n";

    /// Map partition_size to 0..1
    double num_parts_normalized = mapPiecewiseLinearToUnit(partition_size, settings.min_parts_to_lower_base /*10*/, settings.max_parts_to_lower_base /*50*/);

//    std::cerr << "partition_size: " << partition_size << "\n";
//    std::cerr << "num_parts_normalized: " << num_parts_normalized << "\n";

    double combined_ratio = std::min(1.0, age_normalized + num_parts_normalized);

//    std::cerr << "combined_ratio: " << combined_ratio << "\n";

    // 0 < combined_ratio < 1, 所以lowered_base一定在[2,5]之间啊啊
    // combined_ratio越大，lowered_base越小
    //  lerp(a,b,t) 返回 a+t(b-a)
    double lowered_base = interpolateLinear(settings.base, 2.0, combined_ratio);

//    std::cerr << "------- lowered_base: " << lowered_base << "\n";

    // 以上只是求lowered_base
    // sum_size, range_size都是传进来的参数，不会变的
    // base为1, 难道是这个调正后的表达式 > lowered_base??? 。。。。

    
    return (sum_size + range_size * settings.size_fixed_cost_to_add) / (max_size + settings.size_fixed_cost_to_add) >= lowered_base;
}

// 在参数PartsRange中，遍历各种begin, end的组合
void selectWithinPartition(
    const SimpleMergeSelector::PartsRange & parts,
    const size_t max_total_size_to_merge,
    Estimator & estimator,
    const SimpleMergeSelector::Settings & settings,
    double min_size_to_lower_base_log,
    double max_size_to_lower_base_log)
{
    size_t parts_count = parts.size();
    if (parts_count <= 1)
        return;

    /// If the parts in the parts vector are sorted by block number,
    /// it may not be ideal to only select parts for merging from the first N ones.
    /// This is because if there are more than N parts in the partition,
    /// we will not be able to assign a merge for newly created parts.
    /// As a result, the total number of parts within the partition could
    /// grow uncontrollably, similar to a snowball effect.
    /// To address this we will try to assign a merge taking into consideration
    /// only last N parts.
    static constexpr size_t parts_threshold = 1000;   // N=1000
    size_t begin = 0;
    if (parts_count >= parts_threshold)
        begin = parts_count - parts_threshold;
    
    // 遍历begin. 对每个begin，遍历end 
    for (; begin < parts_count; ++begin)
    {
        if (!parts[begin].shall_participate_in_merges)
            continue;

        size_t sum_size = parts[begin].size;
        size_t max_size = parts[begin].size;
        size_t min_age = parts[begin].age;

        for (size_t end = begin + 2; end <= parts_count; ++end)
        {
            assert(end > begin);
            if (settings.max_parts_to_merge_at_once && end - begin > settings.max_parts_to_merge_at_once)  // defaults to 100
                break;

            if (!parts[end - 1].shall_participate_in_merges)
                break;

            size_t cur_size = parts[end - 1].size;
            size_t cur_age = parts[end - 1].age;

            sum_size += cur_size;
            max_size = std::max(max_size, cur_size);
            min_age = std::min(min_age, cur_age);

            // max_total_size_to_merge来自 MergeTreeDataMergerMutator::getMaxSourcePartsSizeForMerge
            // 相关settings, max_bytes_to_merge_at_min_space_in_pool(defaults to 150gb), max_bytes_to_merge_at_max_space_in_pool(defaults to 1mb)
            if (max_total_size_to_merge && sum_size > max_total_size_to_merge)
                break;

            if (allow(sum_size, max_size, min_age, end - begin, parts_count, min_size_to_lower_base_log, max_size_to_lower_base_log, settings))
                estimator.consider(
                    parts.begin() + begin,
                    parts.begin() + end,
                    sum_size,
                    begin == 0 ? 0 : parts[begin - 1].size,   // size_prev_at_left
                    settings);
        }
    }
}

}


SimpleMergeSelector::PartsRange SimpleMergeSelector::select(
    const PartsRanges & parts_ranges,
    size_t max_total_size_to_merge)
{
    Estimator estimator;

    /// Precompute logarithm of settings boundaries, because log function is quite expensive in terms of performance
    const double min_size_to_lower_base_log = log(1 + settings.min_size_to_lower_base); // 。。。
    const double max_size_to_lower_base_log = log(1 + settings.max_size_to_lower_base); // 。。。

    for (const auto & part_range : parts_ranges)
        selectWithinPartition(part_range, max_total_size_to_merge, estimator, settings, min_size_to_lower_base_log, max_size_to_lower_base_log);

    // 是对parts_ranges中每个parts_range 的每个[begin, end)遍历后，得到的score最低的
    return estimator.getBest();
}

}
