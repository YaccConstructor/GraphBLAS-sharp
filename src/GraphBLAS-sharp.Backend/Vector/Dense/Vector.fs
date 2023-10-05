namespace GraphBLAS.FSharp.Backend.Vector.Dense

open Brahma.FSharp
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Objects.ClVector
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Objects.ClCellExtensions

module Vector =
    let map<'a, 'b when 'a: struct and 'b: struct>
        (op: Expr<'a option -> 'b option>)
        (clContext: ClContext)
        workGroupSize
        =

        let map = Map.map op clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (leftVector: ClArray<'a option>) ->

            map processor allocationMode leftVector

    let map2InPlace<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        (clContext: ClContext)
        workGroupSize
        =

        let map2InPlace =
            Map.map2InPlace opAdd clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (resultVector: ClArray<'c option>) ->

            map2InPlace processor leftVector rightVector resultVector

    let map2<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        (clContext: ClContext)
        workGroupSize
        =

        let map2 = Map.map2 opAdd clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) ->

            map2 processor allocationMode leftVector rightVector

    let map2AtLeastOne op clContext workGroupSize =
        map2 (Convert.atLeastOneToOption op) clContext workGroupSize

    let assignByMaskInPlace<'a, 'b when 'a: struct and 'b: struct>
        (maskOp: Expr<'a option -> 'b option -> 'a -> 'a option>)
        (clContext: ClContext)
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
        (maskOp: Expr<'a option -> 'b option -> 'a -> 'a option>)
        (clContext: ClContext)
        workGroupSize
        =

        let assignByMask =
            assignByMaskInPlace maskOp clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (leftVector: ClArray<'a option>) (maskVector: ClArray<'b option>) (value: ClCell<'a>) ->
            let resultVector =
                clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, leftVector.Length)

            assignByMask processor leftVector maskVector value resultVector

            resultVector

    let assignBySparseMaskInPlace<'a, 'b when 'a: struct and 'b: struct>
        (maskOp: Expr<'a option -> 'b option -> 'a -> 'a option>)
        (clContext: ClContext)
        workGroupSize
        =

        let fillSubVectorKernel =
            <@ fun (ndRange: Range1D) resultLength (leftVector: ClArray<'a option>) (maskVectorIndices: ClArray<int>) (maskVectorValues: ClArray<'b>) (value: ClCell<'a>) (resultVector: ClArray<'a option>) ->

                let gid = ndRange.GlobalID0

                if gid < resultLength then
                    let i = maskVectorIndices.[gid]
                    resultVector.[i] <- (%maskOp) leftVector.[i] (Some maskVectorValues.[gid]) value.Value @>

        let kernel = clContext.Compile(fillSubVectorKernel)

        fun (processor: MailboxProcessor<_>) (leftVector: ClArray<'a option>) (maskVector: Sparse<'b>) (value: ClCell<'a>) (resultVector: ClArray<'a option>) ->

            let ndRange =
                Range1D.CreateValid(maskVector.NNZ, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            maskVector.NNZ
                            leftVector
                            maskVector.Indices
                            maskVector.Values
                            value
                            resultVector)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

    let toSparse<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let scatterValues =
            Common.Scatter.lastOccurrence clContext workGroupSize

        let scatterIndices =
            Common.Scatter.lastOccurrence clContext workGroupSize

        let getBitmap =
            Map.map (Map.option 1 0) clContext workGroupSize

        let prefixSum =
            Common.PrefixSum.standardExcludeInPlace clContext workGroupSize

        let allIndices =
            ClArray.init Map.id clContext workGroupSize

        let allValues =
            Map.map (Map.optionToValueOrZero Unchecked.defaultof<'a>) clContext workGroupSize

        fun (processor: MailboxProcessor<_>) allocationMode (vector: ClArray<'a option>) ->

            let positions = getBitmap processor DeviceOnly vector

            let resultLength =
                (prefixSum processor positions)
                    .ToHostAndFree(processor)

            // compute result indices
            let resultIndices =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(allocationMode, resultLength)

            let allIndices =
                allIndices processor DeviceOnly vector.Length

            scatterIndices processor positions allIndices resultIndices

            processor.Post <| Msg.CreateFreeMsg<_>(allIndices)

            // compute result values
            let resultValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'a>(allocationMode, resultLength)

            let allValues = allValues processor DeviceOnly vector

            scatterValues processor positions allValues resultValues

            processor.Post <| Msg.CreateFreeMsg<_>(allValues)

            processor.Post <| Msg.CreateFreeMsg<_>(positions)

            { Context = clContext
              Indices = resultIndices
              Values = resultValues
              Size = vector.Length }

    let reduce<'a when 'a: struct> (opAdd: Expr<'a -> 'a -> 'a>) (clContext: ClContext) workGroupSize =

        let choose =
            ClArray.choose Map.id clContext workGroupSize

        let reduce =
            Common.Reduce.reduce opAdd clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClArray<'a option>) ->
            choose processor DeviceOnly vector
            |> function
                | Some values ->
                    let result = reduce processor values

                    processor.Post(Msg.CreateFreeMsg<_>(values))

                    result
                | None -> clContext.CreateClCell Unchecked.defaultof<'a>
