namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Objects.ClContextExtensions

module Map =
    /// <summary>
    /// Builds a new array whose elements are the results of applying the given function
    /// to each of the elements of the array.
    /// </summary>
    /// <param name="op">The function to transform elements of the array.</param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let map<'a, 'b> (op: Expr<'a -> 'b>) (clContext: ClContext) workGroupSize =

        let map =
            <@ fun (ndRange: Range1D) lenght (inputArray: ClArray<'a>) (result: ClArray<'b>) ->

                let gid = ndRange.GlobalID0

                if gid < lenght then
                    result.[gid] <- (%op) inputArray.[gid] @>

        let kernel = clContext.Compile map

        fun (processor: MailboxProcessor<_>) allocationMode (inputArray: ClArray<'a>) ->

            let result =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, inputArray.Length)

            let ndRange =
                Range1D.CreateValid(inputArray.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange inputArray.Length inputArray result))

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            result

    /// <summary>
    /// Changes elements of the input array, applying the given function
    /// to each element of the array.
    /// </summary>
    /// <param name="op">The function to transform elements of the array.</param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let mapInPlace<'a> (op: Expr<'a -> 'a>) (clContext: ClContext) workGroupSize =

        let map =
            <@ fun (ndRange: Range1D) length (inputArray: ClArray<'a>) ->

                let gid = ndRange.GlobalID0

                if gid < length then
                    inputArray.[gid] <- (%op) inputArray.[gid] @>

        let kernel = clContext.Compile map

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) ->

            let ndRange =
                Range1D.CreateValid(inputArray.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange inputArray.Length inputArray))

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    /// <summary>
    /// Builds a new array whose elements are the results of applying the given function
    /// to the corresponding pairs of values, where the first element of pair is from the given array
    /// and the second element is the given value.
    /// </summary>
    /// <param name="op">The function to transform elements of the array.</param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let mapWithValue<'a, 'b, 'c> (clContext: ClContext) workGroupSize (op: Expr<'a -> 'b -> 'c>) =

        let map =
            <@ fun (ndRange: Range1D) lenght (value: ClCell<'a>) (inputArray: ClArray<'b>) (result: ClArray<'c>) ->

                let gid = ndRange.GlobalID0

                if gid < lenght then
                    result.[gid] <- (%op) value.Value inputArray.[gid] @>

        let kernel = clContext.Compile map

        fun (processor: MailboxProcessor<_>) allocationMode (value: 'a) (inputArray: ClArray<'b>) ->

            let result =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, inputArray.Length)

            let valueClCell = value |> clContext.CreateClCell

            let ndRange =
                Range1D.CreateValid(inputArray.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange inputArray.Length valueClCell inputArray result)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            result

    /// <summary>
    /// Fills the third given array with the results of applying the given function
    /// to the corresponding elements of the first two given arrays pairwise.
    /// </summary>
    /// <remarks>
    /// The first two input arrays must have the same lengths.
    /// </remarks>
    /// <param name="map">The function to transform the pairs of the input elements.</param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let map2InPlace<'a, 'b, 'c> (map: Expr<'a -> 'b -> 'c>) (clContext: ClContext) workGroupSize =

        let kernel =
            <@ fun (ndRange: Range1D) length (leftArray: ClArray<'a>) (rightArray: ClArray<'b>) (resultArray: ClArray<'c>) ->

                let gid = ndRange.GlobalID0

                if gid < length then

                    resultArray.[gid] <- (%map) leftArray.[gid] rightArray.[gid] @>

        let kernel = clContext.Compile kernel

        fun (processor: MailboxProcessor<_>) (leftArray: ClArray<'a>) (rightArray: ClArray<'b>) (resultArray: ClArray<'c>) ->

            let ndRange =
                Range1D.CreateValid(resultArray.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange resultArray.Length leftArray rightArray resultArray)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    /// <summary>
    /// Builds a new array whose elements are the results of applying the given function
    /// to the corresponding elements of the two given arrays pairwise.
    /// </summary>
    /// <remarks>
    /// The two input arrays must have the same lengths.
    /// </remarks>
    /// <param name="map">The function to transform the pairs of the input elements.</param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let map2<'a, 'b, 'c> map (clContext: ClContext) workGroupSize =
        let map2 =
            map2InPlace<'a, 'b, 'c> map clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (leftArray: ClArray<'a>) (rightArray: ClArray<'b>) ->

            let resultArray =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, leftArray.Length)

            map2 processor leftArray rightArray resultArray

            resultArray
