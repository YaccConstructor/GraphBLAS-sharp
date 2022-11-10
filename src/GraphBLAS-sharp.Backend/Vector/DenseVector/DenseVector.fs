namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Quotations

module DenseVector =
    let elementWise<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        (workGroupSize: int)
        =

        let eWiseAdd =
            <@ fun (ndRange: Range1D) resultLength (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (resultVector: ClArray<'c option>) ->

                let gid = ndRange.GlobalID0

                if gid < resultLength then
                     resultVector.[gid] <- (%opAdd) leftVector.[gid] rightVector.[gid] @>

        let kernel = clContext.Compile(eWiseAdd)

        fun (processor: MailboxProcessor<_>) (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) ->

            let resultVector =
                clContext.CreateClArray(
                    leftVector.Length,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let ndRange =
                Range1D.CreateValid(leftVector.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange leftVector.Length leftVector rightVector resultVector)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultVector

    let elementWiseAtLeastOne<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        (workGroupSize: int)
        =

        let eWiseAdd =
            <@ fun (ndRange: Range1D) resultLength (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (resultVector: ClArray<'c option>) ->

                let gid = ndRange.GlobalID0

                if gid < resultLength then
                    match leftVector.[gid], rightVector.[gid] with
                    | Some left, Some right -> resultVector.[gid] <- (%opAdd) (Both(left, right))
                    | Some left, None -> resultVector.[gid] <- (%opAdd) (Left left)
                    | None, Some right -> resultVector.[gid] <- (%opAdd) (Right right)
                    | _ -> resultVector.[gid] <- None @>

        let kernel = clContext.Compile(eWiseAdd)

        fun (processor: MailboxProcessor<_>) (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) ->

            let resultVector =
                clContext.CreateClArray(
                    leftVector.Length,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let ndRange =
                Range1D.CreateValid(leftVector.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange leftVector.Length leftVector rightVector resultVector)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultVector

    let fillSubVector<'a, 'b when 'a: struct and 'b: struct> (clContext: ClContext) (workGroupSize: int) (scalar: 'a) =

        let eWiseAdd =
            elementWiseAtLeastOne clContext (StandardOperations.maskAtLeastOne scalar) workGroupSize

        fun (processor: MailboxProcessor<_>) (leftVector: ClArray<'a option>) (maskVector: ClArray<'b option>) ->

            let clScalar = clContext.CreateClCell scalar

            let resultVector = eWiseAdd processor leftVector maskVector

            processor.Post(Msg.CreateFreeMsg<_>(maskVector))

            processor.Post(Msg.CreateFreeMsg<_>(clScalar))

            resultVector

    let complemented<'a when 'a: struct> (clContext: ClContext) (workGroupSize: int) =

        let complemented =
            <@ fun (ndRange: Range1D) length (inputArray: ClArray<'a option>) (defaultValue: ClCell<'a>) (resultArray: ClArray<'a option>) ->

                let gid = ndRange.GlobalID0

                if gid < length then
                    match inputArray.[gid] with
                    | None -> resultArray.[gid] <- Some defaultValue.Value
                    | _ -> () @>


        let kernel = clContext.Compile(complemented)

        let create =
            ClArray.zeroCreate clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClArray<'a option>) ->

            let length = vector.Length

            let resultArray = create processor length

            let defaultValue =
                clContext.CreateClCell Unchecked.defaultof<'a>

            let ndRange =
                Range1D.CreateValid(length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange length vector defaultValue resultArray)
            )

            processor.Post(Msg.CreateRunMsg(kernel))

            processor.Post(Msg.CreateFreeMsg(defaultValue))

            resultArray

    let getBitmap<'a when 'a: struct> (clContext: ClContext) (workGroupSize: int) =

        let getPositions =
            <@ fun (ndRange: Range1D) length (vector: ClArray<'a option>) (positions: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                if gid < length then
                    match vector.[gid] with
                    | Some _ -> positions.[gid] <- 1
                    | None -> positions.[gid] <- 0 @>

        let kernel = clContext.Compile(getPositions)

        fun (processor: MailboxProcessor<_>) (vector: ClArray<'a option>) ->

            let positions =
                clContext.CreateClArray(
                    vector.Length,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let ndRange =
                Range1D.CreateValid(vector.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange vector.Length vector positions))

            processor.Post(Msg.CreateRunMsg(kernel))

            positions

    let private getValuesAndIndices<'a when 'a: struct> (clContext: ClContext) (workGroupSize: int) =

        let getValuesAndIndices =
            <@ fun (ndRange: Range1D) length (denseVector: ClArray<'a option>) (positions: ClArray<int>) (resultValues: ClArray<'a>) (resultIndices: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                if gid = length - 1
                   || gid < length
                      && positions.[gid] <> positions.[gid + 1] then
                    let index = positions.[gid]

                    match denseVector.[gid] with
                    | Some value ->
                        resultValues.[index] <- value
                        resultIndices.[index] <- gid
                    | None -> () @>


        let kernel = clContext.Compile(getValuesAndIndices)

        let getPositions = getBitmap clContext workGroupSize

        let prefixSum =
            ClArray.prefixSumExcludeInplace clContext workGroupSize

        let resultLength = Array.zeroCreate 1

        fun (processor: MailboxProcessor<_>) (vector: ClArray<'a option>) ->

            let positions = getPositions processor vector

            let resultLengthGpu = clContext.CreateClCell 0

            let _, r =
                prefixSum processor positions resultLengthGpu

            let resultLength =
                let res =
                    processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(r, resultLength, ch))

                processor.Post(Msg.CreateFreeMsg<_>(r))

                res.[0]

            let resultValues =
                clContext.CreateClArray<'a>(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let resultIndices =
                clContext.CreateClArray<int>(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let ndRange =
                Range1D.CreateValid(vector.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange vector.Length vector positions resultValues resultIndices)
            )

            processor.Post(Msg.CreateRunMsg(kernel))

            processor.Post(Msg.CreateFreeMsg<_>(positions))

            resultValues, resultIndices

    let toCoo<'a when 'a: struct> (clContext: ClContext) (workGroupSize: int) =

        let getValuesAndIndices =
            getValuesAndIndices clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClArray<'a option>) ->

            let values, indices = getValuesAndIndices processor vector

            { Context = clContext
              Indices = indices
              Values = values
              Size = vector.Length }

    let reduce<'a when 'a: struct> (clContext: ClContext) (workGroupSize: int) (opAdd: Expr<'a -> 'a -> 'a>) =

        let getValuesAndIndices =
            getValuesAndIndices clContext workGroupSize

        let reduce = Reduce.run clContext workGroupSize opAdd

        fun (processor: MailboxProcessor<_>) (vector: ClArray<'a option>) ->

            let values, indices = getValuesAndIndices processor vector

            let result = reduce processor values

            processor.Post(Msg.CreateFreeMsg<_>(indices))
            processor.Post(Msg.CreateFreeMsg<_>(values))

            result
