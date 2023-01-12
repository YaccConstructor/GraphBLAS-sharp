namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp
open Microsoft.FSharp.Quotations

module ClArray =
    type ClContext with
        member this.CreateClArrayWithGPUOnlyFlags(size: int) =
            this.CreateClArray(
                size,
                deviceAccessMode = DeviceAccessMode.ReadWrite,
                hostAccessMode = HostAccessMode.NotAccessible,
                allocationMode = AllocationMode.Default
            )

        member this.CreateClArrayWithGPUOnlyFlags(array: 'a []) =
            this.CreateClArray(
                array,
                deviceAccessMode = DeviceAccessMode.ReadWrite,
                hostAccessMode = HostAccessMode.NotAccessible,
                allocationMode = AllocationMode.CopyHostPtr
            )

    let init (initializer: Expr<int -> 'a>) (clContext: ClContext) workGroupSize =

        let init =
            <@ fun (range: Range1D) (outputBuffer: ClArray<'a>) (length: int) ->

                let i = range.GlobalID0

                if i < length then
                    outputBuffer.[i] <- (%initializer) i @>

        let program = clContext.Compile(init)

        fun (processor: MailboxProcessor<_>) (length: int) ->
            // TODO: Выставить нужные флаги
            let outputArray =
                clContext.CreateClArrayWithGPUOnlyFlags(length)

            let kernel = program.GetKernel()

            let ndRange =
                Range1D.CreateValid(length, workGroupSize)

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange outputArray length))
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            outputArray

    let create (clContext: ClContext) workGroupSize =

        let create =
            <@ fun (range: Range1D) (outputBuffer: ClArray<'a>) (length: int) (value: ClCell<'a>) ->

                let i = range.GlobalID0

                if i < length then
                    outputBuffer.[i] <- value.Value @>

        let program = clContext.Compile(create)

        fun (processor: MailboxProcessor<_>) (length: int) (value: 'a) ->
            let value = clContext.CreateClCell(value)

            let outputArray =
                clContext.CreateClArrayWithGPUOnlyFlags(length)

            let kernel = program.GetKernel()

            let ndRange =
                Range1D.CreateValid(length, workGroupSize)

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange outputArray length value))
            processor.Post(Msg.CreateRunMsg<_, _> kernel)
            processor.Post(Msg.CreateFreeMsg(value))

            outputArray

    let zeroCreate (clContext: ClContext) workGroupSize =

        let create = create clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (length: int) -> create processor length Unchecked.defaultof<'a>

    let copy (clContext: ClContext) workGroupSize =
        let copy =

            <@ fun (ndRange: Range1D) (inputArrayBuffer: ClArray<'a>) (outputArrayBuffer: ClArray<'a>) inputArrayLength ->

                let i = ndRange.GlobalID0

                if i < inputArrayLength then
                    outputArrayBuffer.[i] <- inputArrayBuffer.[i] @>

        let program = clContext.Compile(copy)

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) ->
            let ndRange =
                Range1D.CreateValid(inputArray.Length, workGroupSize)

            let outputArray =
                clContext.CreateClArrayWithGPUOnlyFlags inputArray.Length

            let kernel = program.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange inputArray outputArray inputArray.Length)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            outputArray

    let replicate (clContext: ClContext) =

        let replicate =
            <@ fun (ndRange: Range1D) (inputArrayBuffer: ClArray<'a>) (outputArrayBuffer: ClArray<'a>) inputArrayLength outputArrayLength ->

                let i = ndRange.GlobalID0

                if i < outputArrayLength then
                    outputArrayBuffer.[i] <- inputArrayBuffer.[i % inputArrayLength] @>

        let kernel = clContext.Compile(replicate)

        fun (processor: MailboxProcessor<_>) workGroupSize (inputArray: ClArray<'a>) count ->
            let outputArrayLength = inputArray.Length * count

            let outputArray =
                clContext.CreateClArrayWithGPUOnlyFlags outputArrayLength

            let ndRange =
                Range1D.CreateValid(outputArray.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange inputArray outputArray inputArray.Length outputArrayLength)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            outputArray

    /// <summary>
    /// Exclude inplace prefix sum.
    /// </summary>
    /// <example>
    /// <code>
    /// let arr = [| 1; 1; 1; 1 |]
    /// let sum = [| 0 |]
    /// runExcludeInplace clContext workGroupSize processor arr sum <@ (+) @> 0
    /// |> ignore
    /// ...
    /// > val arr = [| 0; 1; 2; 3 |]
    /// > val sum = [| 4 |]
    /// </code>
    /// </example>
    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    ///<param name="processor">.</param>
    ///<param name="inputArray">.</param>
    ///<param name="totalSum">.</param>
    ///<param name="plus">Associative binary operation.</param>
    ///<param name="zero">Zero element for binary operation.</param>
    let prefixSumExcludeInplace = PrefixSum.runExcludeInplace

    /// <summary>
    /// Include inplace prefix sum.
    /// </summary>
    /// <example>
    /// <code>
    /// let arr = [| 1; 1; 1; 1 |]
    /// let sum = [| 0 |]
    /// runExcludeInplace clContext workGroupSize processor arr sum <@ (+) @> 0
    /// |> ignore
    /// ...
    /// > val arr = [| 1; 2; 3; 4 |]
    /// > val sum = [| 4 |]
    /// </code>
    /// </example>
    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    ///<param name="processor">.</param>
    ///<param name="inputArray">.</param>
    ///<param name="totalSum">.</param>
    ///<param name="plus">Associative binary operation.</param>
    ///<param name="zero">Zero element for binary operation.</param>
    let prefixSumIncludeInplace = PrefixSum.runIncludeInplace

    let prefixSumExclude plus (clContext: ClContext) workGroupSize =

        let runExcludeInplace =
            prefixSumExcludeInplace plus clContext workGroupSize

        let copy = copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) (totalSum: ClCell<'a>) (zero: 'a) ->

            let outputArray = copy processor inputArray

            runExcludeInplace processor outputArray totalSum zero

    let prefixSumInclude plus (clContext: ClContext) workGroupSize =

        let runIncludeInplace =
            prefixSumIncludeInplace plus clContext workGroupSize

        let copy = copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) (totalSum: ClCell<'a>) (zero: 'a) ->

            let outputArray = copy processor inputArray

            runIncludeInplace processor outputArray totalSum zero

    let prefixSumBackwardsExcludeInplace plus =
        PrefixSum.runBackwardsExcludeInplace plus

    let prefixSumBackwardsIncludeInplace plus =
        PrefixSum.runBackwardsIncludeInplace plus

    let getUniqueBitmap (clContext: ClContext) =

        let getUniqueBitmap =
            <@ fun (ndRange: Range1D) (inputArray: ClArray<'a>) inputLength (isUniqueBitmap: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if i < inputLength - 1
                   && inputArray.[i] = inputArray.[i + 1] then
                    isUniqueBitmap.[i] <- 0
                else
                    isUniqueBitmap.[i] <- 1 @>

        let kernel = clContext.Compile(getUniqueBitmap)

        fun (processor: MailboxProcessor<_>) workGroupSize (inputArray: ClArray<'a>) ->

            let inputLength = inputArray.Length

            let ndRange =
                Range1D.CreateValid(inputLength, workGroupSize)

            let bitmap =
                clContext.CreateClArrayWithGPUOnlyFlags inputLength

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange inputArray inputLength bitmap))

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            bitmap

    ///<description>Remove duplicates form the given array.</description>
    ///<param name="clContext">Computational context</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    ///<param name="inputArray">Should be sorted.</param>
    let removeDuplications (clContext: ClContext) workGroupSize =

        let scatter = Scatter.runInplace clContext workGroupSize
        let getUniqueBitmap = getUniqueBitmap clContext

        let prefixSumExclude =
            prefixSumExclude <@ (+) @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) ->

            let bitmap =
                getUniqueBitmap processor workGroupSize inputArray

            let sum = clContext.CreateClCell 0

            let positions, sum = prefixSumExclude processor bitmap sum 0

            let resultLength =
                let a = [| 0 |]

                processor.PostAndReply(fun ch -> Msg.CreateToHostMsg(sum, a, ch))
                |> ignore

                processor.Post(Msg.CreateFreeMsg<_>(sum))

                a.[0]

            let outputArray =
                clContext.CreateClArrayWithGPUOnlyFlags resultLength

            scatter processor positions inputArray outputArray

            outputArray
