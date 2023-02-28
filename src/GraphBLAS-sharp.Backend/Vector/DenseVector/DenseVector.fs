namespace GraphBLAS.FSharp.Backend.Vector.Dense

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Quotes
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Predefined
open GraphBLAS.FSharp.Backend.Objects.ClVector
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ClCell

module DenseVector =
    let map2Inplace<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let map2InPlace =
            ClArray.map2Inplace clContext workGroupSize opAdd

        fun (processor: MailboxProcessor<_>) (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) (resultVector: ClArray<'c option>) ->

            map2InPlace processor leftVector rightVector resultVector


    let map2<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let map2 =
            ClArray.map2 clContext workGroupSize opAdd

        fun (processor: MailboxProcessor<_>) allocationMode (leftVector: ClArray<'a option>) (rightVector: ClArray<'b option>) ->

            map2 processor allocationMode leftVector rightVector


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

    let toSparse<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let scatterValues =
            Scatter.runInplace clContext workGroupSize

        let scatterIndices =
            Scatter.runInplace clContext workGroupSize

        let getBitmap =
            ClArray.map clContext workGroupSize
            <| Map.option 1 0

        let prefixSum =
            PrefixSum.standardExcludeInplace clContext workGroupSize

        let allIndices =
            ClArray.init clContext workGroupSize Map.id

        let allValues =
            ClArray.map clContext workGroupSize
            <| Map.optionToValueOrZero Unchecked.defaultof<'a>

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

    let reduce<'a when 'a: struct> (clContext: ClContext) workGroupSize (opAdd: Expr<'a -> 'a -> 'a>) =

        let choose =
            ClArray.choose clContext workGroupSize Map.id

        let reduce =
            Reduce.reduce clContext workGroupSize opAdd

        let containsNonZero =
            ClArray.exists clContext workGroupSize Predicates.isSome

        fun (processor: MailboxProcessor<_>) (vector: ClArray<'a option>) ->

            let notEmpty =
                (containsNonZero processor vector)
                    .ToHostAndFree processor

            if notEmpty then
                let values = choose processor DeviceOnly vector

                let result = reduce processor values

                processor.Post(Msg.CreateFreeMsg<_>(values))

                result

            else
                clContext.CreateClCell Unchecked.defaultof<'a>
