namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Quotations

module DenseVector =
    let private maskWithValue<'a, 'b when 'a: struct and 'b: struct> (clContext: ClContext) (workGroupSize: int) =

        let fillVector =
            <@ fun (ndRange: Range1D) length (maskArray: ClArray<'a option>) (scalar: ClCell<'b>) (resultArray: ClArray<'b option>) ->

                let gid = ndRange.GlobalID0

                if gid < length then
                    match maskArray.[gid] with
                    | Some _ -> resultArray.[gid] <- Some scalar.Value
                    | None -> resultArray.[gid] <- None @>

        let kernel = clContext.Compile(fillVector)

        fun (processor: MailboxProcessor<_>) (maskVector: ClArray<'a option>) (scalarCell: ClCell<'b>) ->

            let resultArray =
                clContext.CreateClArray(
                    maskVector.Length,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let ndRange =
                Range1D.CreateValid(maskVector.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange maskVector.Length maskVector scalarCell resultArray)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultArray

    let elementWiseAddAtLeasOne<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        (workGroupSize: int)
        =

        let eWiseAdd =
            <@ fun (ndRange: Range1D) leftVectorLength rightVectorLength resultLength (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (resultVector: ClArray<'c option>) ->

                let gid = ndRange.GlobalID0

                let mutable leftItem = None
                let mutable rightItem = None

                if gid < leftVectorLength then
                    leftItem <- leftVector.[gid]

                if gid < rightVectorLength then
                    rightItem <- rightVector.[gid]

                if gid < resultLength then
                    match leftItem, rightItem with
                    | Some left, Some right -> resultVector.[gid] <- (%opAdd) (Both(left, right))
                    | Some left, None -> resultVector.[gid] <- (%opAdd) (Left left)
                    | None, Some right -> resultVector.[gid] <- (%opAdd) (Right right)
                    | None, None -> resultVector.[gid] <- None @>

        let kernel = clContext.Compile(eWiseAdd)

        fun (processor: MailboxProcessor<_>) (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) ->

            let resultLength = max leftVector.Length rightVector.Length

            let resultVector =
                clContext.CreateClArray(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.ReadWrite,
                    allocationMode = AllocationMode.Default
                )

            let ndRange =
                Range1D.CreateValid(resultLength, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            leftVector.Length
                            rightVector.Length
                            resultLength
                            leftVector
                            rightVector
                            resultVector)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultVector

    let fillSubVector<'a, 'b when 'a: struct and 'b: struct> (clContext: ClContext) (workGroupSize: int) = //zero

        let eWiseAdd =
            elementWiseAddAtLeasOne clContext StandardOperations.maskAtLeastOne workGroupSize

        let copyWithValue = maskWithValue clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (leftVector: ClArray<'a option>) (maskVector: ClArray<'b option>) (scalar: 'a) ->

            let clScalar = clContext.CreateClCell scalar

            let maskVector =
                copyWithValue processor maskVector clScalar

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

    let getValuesAndIndices<'a when 'a: struct> (clContext: ClContext) (workGroupSize: int) =

        let unzip =
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


        let kernel = clContext.Compile(unzip)

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

            { ClSparseVector.Context = clContext
              Indices = indices
              Values = values
              Size = vector.Length }

    let reduce<'a when 'a: struct> (clContext: ClContext) (workGroupSize: int) (opAdd: Expr<'a -> 'a -> 'a>) zero =

        let getValuesAndIndices =
            getValuesAndIndices clContext workGroupSize

        let reduce =
            Reduce.run clContext workGroupSize opAdd zero

        fun (processor: MailboxProcessor<_>) (vector: ClArray<'a option>) ->

            let values, indices = getValuesAndIndices processor vector

            processor.Post(Msg.CreateFreeMsg<_>(indices))

            reduce processor values
