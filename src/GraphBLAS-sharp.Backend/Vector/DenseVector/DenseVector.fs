namespace GraphBLAS.FSharp.Backend.Vector.Dense

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Quotes
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Predefined
open GraphBLAS.FSharp.Backend.Objects.ClVector
open GraphBLAS.FSharp.Backend.Objects.ClContext

module DenseVector =
    let map2Inplace<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let map2 =
            <@ fun (ndRange: Range1D) resultLength (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (resultVector: ClArray<'c option>) ->

                let gid = ndRange.GlobalID0

                if gid < resultLength then
                    resultVector.[gid] <- (%opAdd) leftVector.[gid] rightVector.[gid] @>

        let kernel = clContext.Compile(map2)

        fun (processor: MailboxProcessor<_>) (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (resultVector: ClArray<'c option>) ->

            let ndRange =
                Range1D.CreateValid(leftVector.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange leftVector.Length leftVector rightVector resultVector)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let map2<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let elementWiseTo =
            map2Inplace clContext opAdd workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) ->
            let resultVector =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, leftVector.Length)

            elementWiseTo processor leftVector rightVector resultVector

            resultVector

    let map2AtLeastOne clContext op workGroupSize =
        map2 clContext (Convert.atLeastOneToOption op) workGroupSize

    let assignByMaskInplace<'a, 'b when 'a: struct and 'b: struct>
        (clContext: ClContext)
        (maskOp: Expr<'a option -> 'b option -> 'a -> 'a option>)
        workGroupSize
        =

        let fillSubVectorKernel =
            <@ fun (ndRange: Range1D) resultLength (leftVector: ClArray<'a option>) (maskVector: ClArray<'b option>) (value: ClCell<'a>) (resultVector: ClArray<'a option>) ->

                let gid = ndRange.GlobalID0

                if gid < resultLength then
                    resultVector.[gid] <- (%maskOp) leftVector.[gid] maskVector.[gid] value.Value @>

        let kernel = clContext.Compile(fillSubVectorKernel)

        fun (processor: MailboxProcessor<_>) (leftVector: ClArray<'a option>) (maskVector: ClArray<'b option>) (value: ClCell<'a>) (resultVector: ClArray<'a option>) ->

            let ndRange =
                Range1D.CreateValid(leftVector.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () -> kernel.KernelFunc ndRange leftVector.Length leftVector maskVector value resultVector)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let assignByMask<'a, 'b when 'a: struct and 'b: struct>
        (clContext: ClContext)
        (maskOp: Expr<'a option -> 'b option -> 'a -> 'a option>)
        workGroupSize
        =

        let assignByMask =
            assignByMaskInplace clContext maskOp workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (leftVector: ClArray<'a option>) (maskVector: ClArray<'b option>) (value: ClCell<'a>) ->
            let resultVector =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, leftVector.Length)

            assignByMask processor leftVector maskVector value resultVector

            resultVector

    let private getBitmap<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let getPositions =
            <@ fun (ndRange: Range1D) length (vector: ClArray<'a option>) (positions: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                if gid < length then
                    match vector.[gid] with
                    | Some _ -> positions.[gid] <- 1
                    | None -> positions.[gid] <- 0 @>

        let kernel = clContext.Compile(getPositions)

        fun (processor: MailboxProcessor<_>) allocationMode (vector: ClArray<'a option>) ->
            let positions =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, vector.Length)

            let ndRange =
                Range1D.CreateValid(vector.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange vector.Length vector positions))

            processor.Post(Msg.CreateRunMsg(kernel))

            positions

    let private getValuesAndIndices<'a when 'a: struct> (clContext: ClContext) workGroupSize =

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

        let resultLength = Array.zeroCreate<int> 1

        fun (processor: MailboxProcessor<_>) allocationMode (vector: ClArray<'a option>) ->

            let positions = getPositions processor DeviceOnly vector

            let resultLengthGpu = clContext.CreateClCell 0

            let _, r =
                prefixSum processor positions resultLengthGpu

            let resultLength =
                let res =
                    processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(r, resultLength, ch))

                processor.Post(Msg.CreateFreeMsg<_>(r))

                res.[0]

            let resultValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'a>(allocationMode, resultLength)

            let resultIndices =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(allocationMode, resultLength)

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

    let toSparse<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let getValuesAndIndices =
            getValuesAndIndices clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (vector: ClArray<'a option>) ->

            let values, indices =
                getValuesAndIndices processor allocationMode vector

            { Context = clContext
              Indices = indices
              Values = values
              Size = vector.Length }

    let reduce<'a when 'a: struct> (clContext: ClContext) workGroupSize (opAdd: Expr<'a -> 'a -> 'a>) =

        let getValuesAndIndices =
            getValuesAndIndices clContext workGroupSize

        let reduce =
            Reduce.reduce clContext workGroupSize opAdd

        fun (processor: MailboxProcessor<_>) (vector: ClArray<'a option>) ->

            try
                let values, indices =
                    getValuesAndIndices processor DeviceOnly vector

                let result = reduce processor values

                processor.Post(Msg.CreateFreeMsg<_>(indices))
                processor.Post(Msg.CreateFreeMsg<_>(values))

                result
            with
            | ex when ex.Message = "InvalidBufferSize" -> clContext.CreateClCell Unchecked.defaultof<'a>
            | ex -> raise ex
