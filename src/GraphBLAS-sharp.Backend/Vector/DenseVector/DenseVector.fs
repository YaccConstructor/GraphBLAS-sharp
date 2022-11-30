namespace GraphBLAS.FSharp.Backend.DenseVector

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Predefined

module DenseVector =
    let containsNonZero<'a when 'a: struct> (clContext: ClContext) (workGroupSize: int) =

        let containsNonZero =
            <@ fun (ndRange: Range1D) length (vector: ClArray<'a option>) (result: ClCell<bool>) ->

                let gid = ndRange.GlobalID0

                if gid < length then
                    match vector.[gid] with
                    | Some _ -> result.Value <- true
                    | _ -> () @>

        let kernel = clContext.Compile containsNonZero

        fun (processor: MailboxProcessor<_>) (vector: ClArray<'a option>) ->

            let result = clContext.CreateClCell false

            let ndRange =
                Range1D.CreateValid(vector.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange vector.Length vector result))

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            result

    let elementWise<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        (workGroupSize: int)
        =

        let elementWise =
            <@ fun (ndRange: Range1D) resultLength (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (resultVector: ClArray<'c option>) ->

                let gid = ndRange.GlobalID0

                if gid < resultLength then
                    resultVector.[gid] <- (%opAdd) leftVector.[gid] rightVector.[gid] @>

        let kernel = clContext.Compile(elementWise)

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

    let elementWiseAtLeastOne clContext op workGroupSize =
        elementWise clContext (StandardOperations.atLeastOneToOption op) workGroupSize

    let fillSubVector<'a, 'b when 'a: struct and 'b: struct>
        (clContext: ClContext)
        (maskOp: Expr<'a option -> 'b option -> 'a -> 'a option>)
        (workGroupSize: int)
        =

        let fillSubVectorKernel =
            <@ fun (ndRange: Range1D) resultLength (leftVector: ClArray<'a option>) (maskVector: ClArray<'b option>) (value: ClCell<'a>) (resultVector: ClArray<'a option>) ->

                let gid = ndRange.GlobalID0

                if gid < resultLength then
                    resultVector.[gid] <- (%maskOp) leftVector.[gid] maskVector.[gid] value.Value @>

        let kernel = clContext.Compile(fillSubVectorKernel)

        fun (processor: MailboxProcessor<_>) (leftVector: ClArray<'a option>) (maskVector: ClArray<'b option>) (value: ClCell<'a>) ->
            let resultVector =
                clContext.CreateClArray<'a option>(
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
                    (fun () -> kernel.KernelFunc ndRange leftVector.Length leftVector maskVector value resultVector)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultVector

    let private getBitmap<'a when 'a: struct> (clContext: ClContext) (workGroupSize: int) =

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
            PrefixSum.standardExcludeInplace clContext workGroupSize

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

    let toSparse<'a when 'a: struct> (clContext: ClContext) (workGroupSize: int) =

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
