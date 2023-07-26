namespace GraphBLAS.FSharp

open Brahma.FSharp
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common

module Common =
    module Bitonic =
        /// <summary>
        /// Sorts in-place input array of values by their 2d indices,
        /// which are stored in two given arrays of keys: rows and columns.
        /// When comparing, it first looks at rows, then columns.
        /// </summary>
        /// <example>
        /// <code>
        /// let rows = [| 0; 0; 3; 2; 1; 0; 5 |]
        /// let columns = [| 0; 2; 1; 2; 0; 3; 5; |]
        /// let values = [| 1.9; 2.8; 3.7; 4.6; 5.5; 6.4; 7.3; |]
        /// sortKeyValuesInplace clContext 32 processor rows columns values
        /// ...
        /// > val rows = [| 0; 0; 0; 1; 2; 3; 5 |]
        /// > let columns = [| 0; 2; 3; 0; 2; 1; 5; |]
        /// > val values = [| 1.9; 2.8; 6.4; 5.5; 4.6; 3.7; 7.3 |]
        /// </code>
        /// </example>
        let sortKeyValuesInplace<'n, 'a when 'n: comparison> =
            Sort.Bitonic.sortKeyValuesInplace<'n, 'a>

    module Radix =
        /// <summary>
        /// Sorts stable input array of values by given integer keys.
        /// </summary>
        /// <example>
        /// <code>
        /// let keys = [| 0; 4; 3; 1; 2; 6; 5 |]
        /// let values = [| 1.9; 2.8; 3.7; 4.6; 5.5; 6.4; 7.3; |]
        /// runByKeysStandard clContext 32 processor keys values
        /// ...
        /// > val keys = [| 0; 1; 2; 3; 4; 5; 6 |]
        /// > val values = [| 1.9; 4.6; 5.5; 3.7; 2.8; 7.3; 6.4 |]
        /// </code>
        /// </example>
        let runByKeysStandard = Sort.Radix.runByKeysStandard

        /// <summary>
        /// Sorts stable input array of integer keys.
        /// </summary>
        /// <example>
        /// <code>
        /// let keys = [| 0; 4; 3; 1; 2; 6; 5 |]
        /// standardRunKeysOnly clContext 32 processor keys
        /// ...
        /// > val keys = [| 0; 1; 2; 3; 4; 5; 6 |]
        /// </code>
        /// </example>
        let standardRunKeysOnly = Sort.Radix.standardRunKeysOnly

    module Gather =
        /// <summary>
        /// Fills the given output array using the given value array and a function. The function maps old position
        /// of each element of the value array to its position in the output array.
        /// </summary>
        /// <remarks>
        /// If index is out of bounds, the value will be ignored.
        /// </remarks>
        /// <example>
        /// <code>
        /// let positions = [| 1; 0; 2; 6; 4; 3; 5 |]
        /// let values = [| 1.9; 2.8; 3.7; 4.6; 5.5; 6.4; 7.3; |]
        /// run clContext 32 processor positions values result
        /// ...
        /// > val result = [| 2.8; 1.9; 3.7; 7.3; 5.5; 4.6; 6.4 |]
        /// </code>
        /// </example>
        let runInit positionMap (clContext: ClContext) workGroupSize =
            Gather.runInit positionMap clContext workGroupSize

        /// <summary>
        /// Fills the given output array using the given value and position arrays. Array of positions indicates
        /// which element from the value array should be in each position of the output array.
        /// </summary>
        /// <remarks>
        /// If index is out of bounds, the value will be ignored.
        /// </remarks>
        /// <example>
        /// <code>
        /// let positions = [| 1; 0; 2; 6; 4; 3; 5 |]
        /// let values = [| 1.9; 2.8; 3.7; 4.6; 5.5; 6.4; 7.3; |]
        /// run clContext 32 processor positions values result
        /// ...
        /// > val result = [| 2.8; 1.9; 3.7; 7.3; 5.5; 4.6; 6.4 |]
        /// </code>
        /// </example>
        let run (clContext: ClContext) workGroupSize = Gather.run clContext workGroupSize

    module Scatter =
        /// <summary>
        /// Creates a new array from the given one where it is indicated
        /// by the array of positions at which position in the new array
        /// should be a value from the given one.
        /// </summary>
        /// <remarks>
        /// Every element of the positions array must not be less than the previous one.
        /// If there are several elements with the same indices, the FIRST one of them will be at the common index.
        /// If index is out of bounds, the value will be ignored.
        /// </remarks>
        /// <example>
        /// <code>
        /// let positions = [| 0; 0; 1; 1; 1; 2; 3; 3; 4 |]
        /// let values = [| 1.9; 2.8; 3.7; 4.6; 5.5; 6.4; 7.3; 8.2; 9.1 |]
        /// run clContext 32 processor positions values result
        /// ...
        /// > val result = [| 1,9; 3.7; 6.4; 7.3; 9.1 |]
        /// </code>
        /// </example>
        let firstOccurrence clContext = Scatter.firstOccurrence clContext

        /// <summary>
        /// Creates a new array from the given one where it is indicated by the array of positions at which position in the new array
        /// should be a value from the given one.
        /// </summary>
        /// <remarks>
        /// Every element of the positions array must not be less than the previous one.
        /// If there are several elements with the same indices, the LAST one of them will be at the common index.
        /// If index is out of bounds, the value will be ignored.
        /// </remarks>
        /// <example>
        /// <code>
        /// let positions = [| 0; 0; 1; 1; 1; 2; 3; 3; 4 |]
        /// let values = [| 1.9; 2.8; 3.7; 4.6; 5.5; 6.4; 7.3; 8.2; 9.1 |]
        /// run clContext 32 processor positions values result
        /// ...
        /// > val result = [| 2.8; 5.5; 6.4; 8.2; 9.1 |]
        /// </code>
        /// </example>
        let lastOccurrence clContext = Scatter.lastOccurrence clContext

        /// <summary>
        /// Creates a new array from the given one where it is indicated by the array of positions at which position in the new array
        /// should be a values obtained by applying the mapping to the global id.
        /// </summary>
        /// <remarks>
        /// Every element of the positions array must not be less than the previous one.
        /// If there are several elements with the same indices, the FIRST one of them will be at the common index.
        /// If index is out of bounds, the value will be ignored.
        /// </remarks>
        /// <example>
        /// <code>
        /// let positions = [| 0; 0; 1; 1; 1; 2; 3; 3; 4 |]
        /// let valueMap = id
        /// run clContext 32 processor positions values result
        /// ...
        /// > val result = [| 0; 2; 5; 6; 8 |]
        /// </code>
        /// </example>
        /// <param name="valueMap">Maps global id to a value</param>
        let initFirstOccurrence valueMap = Scatter.initFirstOccurrence valueMap

        /// <summary>
        /// Creates a new array from the given one where it is indicated by the array of positions at which position in the new array
        /// should be a values obtained by applying the mapping to the global id.
        /// </summary>
        /// <remarks>
        /// Every element of the positions array must not be less than the previous one.
        /// If there are several elements with the same indices, the LAST one of them will be at the common index.
        /// If index is out of bounds, the value will be ignored.
        /// </remarks>
        /// <example>
        /// <code>
        /// let positions = [| 0; 0; 1; 1; 1; 2; 3; 3; 4 |]
        /// let valueMap = id
        /// run clContext 32 processor positions values result
        /// ...
        /// > val result = [| 1; 4; 5; 7; 8 |]
        /// </code>
        /// </example>
        /// <param name="valueMap">Maps global id to a value</param>
        let initLastOccurrence valueMap = Scatter.initLastOccurrence valueMap

    module PrefixSum =
        /// <summary>
        /// Exclude in-place prefix sum.
        /// </summary>
        /// <example>
        /// <code>
        /// let arr = [| 1; 1; 1; 1 |]
        /// let sum = [| 0 |]
        /// runExcludeInplace clContext workGroupSize processor arr sum (+) 0
        /// |> ignore
        /// ...
        /// > val arr = [| 0; 1; 2; 3 |]
        /// > val sum = [| 4 |]
        /// </code>
        /// </example>
        /// <param name="clContext">ClContext.</param>
        /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
        /// <param name="plus">Associative binary operation.</param>
        /// <param name="zero">Zero element for binary operation.</param>
        let runExcludeInPlace plus = PrefixSum.runExcludeInPlace plus

        /// <summary>
        /// Include in-place prefix sum.
        /// </summary>
        /// <example>
        /// <code>
        /// let arr = [| 1; 1; 1; 1 |]
        /// let sum = [| 0 |]
        /// runExcludeInplace clContext workGroupSize processor arr sum (+) 0
        /// |> ignore
        /// ...
        /// > val arr = [| 0; 1; 2; 3 |]
        /// > val sum = [| 4 |]
        /// </code>
        /// </example>
        /// <param name="clContext">ClContext.</param>
        /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
        /// <param name="plus">Associative binary operation.</param>
        /// <param name="zero">Zero element for binary operation.</param>
        let runIncludeInPlace plus = PrefixSum.runIncludeInPlace plus

        /// <summary>
        /// Exclude in-place prefix sum. Array is scanned starting from the end.
        /// </summary>
        /// <param name="clContext">ClContext.</param>
        /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
        /// <param name="plus">Associative binary operation.</param>
        /// <param name="zero">Zero element for binary operation.</param>
        let runBackwardsExcludeInPlace plus =
            PrefixSum.runBackwardsExcludeInPlace plus

        /// <summary>
        /// Include in-place prefix sum. Array is scanned starting from the end.
        /// </summary>
        /// <param name="clContext">ClContext.</param>
        /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
        /// <param name="plus">Associative binary operation.</param>
        /// <param name="zero">Zero element for binary operation.</param>
        let runBackwardsIncludeInPlace plus =
            PrefixSum.runBackwardsIncludeInPlace plus

        /// <summary>
        /// Exclude in-place prefix sum of integer array with addition operation and start value that is equal to 0.
        /// </summary>
        /// <example>
        /// <code>
        /// let arr = [| 1; 1; 1; 1 |]
        /// let sum = [| 0 |]
        /// runExcludeInplace clContext workGroupSize processor arr sum (+) 0
        /// |> ignore
        /// ...
        /// > val arr = [| 0; 1; 2; 3 |]
        /// > val sum = [| 4 |]
        /// </code>
        /// </example>
        let standardExcludeInPlace = PrefixSum.standardExcludeInPlace

        /// <summary>
        /// Include in-place prefix sum of integer array with addition operation and start value that is equal to 0.
        /// </summary>
        /// <example>
        /// <code>
        /// let arr = [| 1; 1; 1; 1 |]
        /// let sum = [| 0 |]
        /// runIncludeInplace clContext workGroupSize processor arr sum (+) 0
        /// |> ignore
        /// ...
        /// > val arr = [| 1; 2; 3; 4 |]
        /// > val sum = [| 4 |]
        /// </code>
        /// </example>
        /// <param name="clContext">ClContext.</param>
        /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
        let standardIncludeInPlace = PrefixSum.standardIncludeInPlace

        module ByKey =
            /// <summary>
            /// Exclude scan by key.
            /// </summary>
            /// <example>
            /// <code>
            /// let arr = [| 1; 1; 1; 1; 1; 1|]
            /// let keys = [| 1; 2; 2; 2; 3; 3 |]
            /// ...
            /// > val result = [| 0; 0; 1; 2; 0; 1 |]
            /// </code>
            /// </example>
            let sequentialExclude op = PrefixSum.ByKey.sequentialExclude op

            /// <summary>
            /// Include scan by key.
            /// </summary>
            /// <example>
            /// <code>
            /// let arr = [| 1; 1; 1; 1; 1; 1|]
            /// let keys = [| 1; 2; 2; 2; 3; 3 |]
            /// ...
            /// > val result = [| 1; 1; 2; 3; 1; 2 |]
            /// </code>
            /// </example>
            let sequentialInclude op = PrefixSum.ByKey.sequentialInclude op

    module Reduce =
        /// <summary>
        /// Summarizes array elements.
        /// </summary>
        /// <param name="clContext">ClContext.</param>
        /// <param name="workGroupSize">Work group size.</param>
        /// <param name="op">Summation operation.</param>
        /// <param name="zero">Neutral element for summation.</param>
        let sum op zero (clContext: ClContext) workGroupSize =
            Reduce.sum op zero clContext workGroupSize

        /// <summary>
        /// Reduces an array of values.
        /// </summary>
        /// <param name="clContext">ClContext.</param>
        /// <param name="workGroupSize">Work group size.</param>
        /// <param name="op">Reduction operation.</param>
        let reduce op (clContext: ClContext) workGroupSize =
            Reduce.reduce op clContext workGroupSize

        /// <summary>
        /// Reduction of an array of values by an array of keys.
        /// </summary>
        module ByKey =
            /// <summary>
            /// Reduces an array of values by key using a single work item.
            /// </summary>
            /// <param name="clContext">ClContext.</param>
            /// <param name="workGroupSize">Work group size.</param>
            /// <param name="reduceOp">Operation for reducing values.</param>
            /// <remarks>
            /// The length of the result must be calculated in advance.
            /// </remarks>
            let sequential (reduceOp: Expr<'a -> 'a -> 'a>) (clContext: ClContext) workGroupSize =
                Reduce.ByKey.sequential reduceOp clContext workGroupSize

            /// <summary>
            /// Reduces values by key. Each segment is reduced by one work item.
            /// </summary>
            /// <param name="clContext">ClContext.</param>
            /// <param name="workGroupSize">Work group size.</param>
            /// <param name="reduceOp">Operation for reducing values.</param>
            /// <remarks>
            /// The length of the result must be calculated in advance.
            /// </remarks>
            let segmentSequential (reduceOp: Expr<'a -> 'a -> 'a>) (clContext: ClContext) workGroupSize =
                Reduce.ByKey.segmentSequential reduceOp clContext workGroupSize

            /// <summary>
            /// Reduces values by key. One work group participates in the reduction.
            /// </summary>
            /// <param name="clContext">ClContext.</param>
            /// <param name="workGroupSize">Work group size.</param>
            /// <param name="reduceOp">Operation for reducing values.</param>
            /// <remarks>
            /// Reduces an array of values that does not exceed the size of the workgroup.
            /// The length of the result must be calculated in advance.
            /// </remarks>
            let oneWorkGroupSegments (reduceOp: Expr<'a -> 'a -> 'a>) (clContext: ClContext) workGroupSize =
                Reduce.ByKey.oneWorkGroupSegments reduceOp clContext workGroupSize

            module Option =
                /// <summary>
                /// Reduces values by key. Each segment is reduced by one work item.
                /// </summary>
                /// <param name="clContext">ClContext.</param>
                /// <param name="workGroupSize">Work group size.</param>
                /// <param name="reduceOp">Operation for reducing values.</param>
                /// <remarks>
                /// The length of the result must be calculated in advance.
                /// </remarks>
                let segmentSequential<'a> (reduceOp: Expr<'a -> 'a -> 'a option>) (clContext: ClContext) workGroupSize =
                    Reduce.ByKey.Option.segmentSequential reduceOp clContext workGroupSize

        module ByKey2D =
            /// <summary>
            /// Reduces an array of values by 2D keys using a single work item.
            /// </summary>
            /// <param name="clContext">ClContext.</param>
            /// <param name="workGroupSize">Work group size.</param>
            /// <param name="reduceOp">Operation for reducing values.</param>
            /// <remarks>
            /// The length of the result must be calculated in advance.
            /// </remarks>
            let sequential (reduceOp: Expr<'a -> 'a -> 'a>) (clContext: ClContext) workGroupSize =
                Reduce.ByKey2D.sequential reduceOp clContext workGroupSize

            /// <summary>
            /// Reduces values by key. Each segment is reduced by one work item.
            /// </summary>
            /// <param name="clContext">ClContext.</param>
            /// <param name="workGroupSize">Work group size.</param>
            /// <param name="reduceOp">Operation for reducing values.</param>
            /// <remarks>
            /// The length of the result must be calculated in advance.
            /// </remarks>
            let segmentSequential (reduceOp: Expr<'a -> 'a -> 'a>) (clContext: ClContext) workGroupSize =
                Reduce.ByKey2D.segmentSequential reduceOp clContext workGroupSize

            module Option =
                /// <summary>
                /// Reduces values by key. Each segment is reduced by one work item.
                /// </summary>
                /// <param name="clContext">ClContext.</param>
                /// <param name="workGroupSize">Work group size.</param>
                /// <param name="reduceOp">Operation for reducing values.</param>
                /// <remarks>
                /// The length of the result must be calculated in advance.
                /// </remarks>
                let segmentSequential (reduceOp: Expr<'a -> 'a -> 'a option>) (clContext: ClContext) workGroupSize =
                    Reduce.ByKey2D.Option.segmentSequential reduceOp clContext workGroupSize
