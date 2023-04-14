namespace GraphBLAS.FSharp.Backend.Vector.Sparse

open Brahma.FSharp
open Microsoft.FSharp.Control
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClVector
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Quotes
open FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects.ClCell
open GraphBLAS.FSharp.Backend.Common

module internal Map =
    let preparePositions<'a, 'b> (clContext: ClContext) workGroupSize opAdd =
        // we can decrease memory requirements by two pass map (like choose)
        let preparePositions (op: Expr<'a option -> 'b option>) =
            <@ fun (ndRange: Range1D) dataLength vectorLength (values: ClArray<'a>) (indices: ClArray<int>) (resultBitmap: ClArray<int>) (resultValues: ClArray<'b>) (resultIndices: ClArray<int>) ->

                let gid = ndRange.GlobalID0

                if gid < vectorLength then

                    let value =
                        (%Search.Bin.byKey) dataLength gid indices values

                    match (%op) value with
                    | Some resultValue ->
                        resultValues.[gid] <- resultValue
                        resultIndices.[gid] <- gid

                        resultBitmap.[gid] <- 1
                    | None -> resultBitmap.[gid] <- 0 @>

        let kernel =
            clContext.Compile <| preparePositions opAdd

        fun (processor: MailboxProcessor<_>) (vector: ClVector.Sparse<'a>) ->

            let resultBitmap =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, vector.Size)

            let resultIndices =
                clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, vector.Size)

            let resultValues =
                clContext.CreateClArrayWithSpecificAllocationMode<'b>(DeviceOnly, vector.Size)

            let ndRange =
                Range1D.CreateValid(vector.Size, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            vector.Values.Length
                            vector.Size
                            vector.Values
                            vector.Indices
                            resultBitmap
                            resultValues
                            resultIndices)
            )

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            resultBitmap, resultValues, resultIndices

    let run<'a, 'b when 'a: struct and 'b: struct and 'b: equality>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option>)
        workGroupSize
        =

        let preparePositions =
            preparePositions clContext workGroupSize opAdd

        let setPositions =
            Common.setPositions<'b> clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (vector: ClVector.Sparse<'a>) ->

            let bitmap, values, indices = preparePositions queue vector

            let resultValues, resultIndices =
                setPositions queue allocationMode values indices bitmap

            bitmap.Free queue
            values.Free queue
            indices.Free queue

            { Context = clContext
              Indices = resultIndices
              Values = resultValues
              Size = vector.Size }

    module AtLeastOne =
        let run (clContext: ClContext) workGroupSize op =

            let getOptionBitmap =
                ClArray.map clContext workGroupSize
                <| Map.chooseBitmap op

            let prefixSum =
                PrefixSum.standardExcludeInplace clContext workGroupSize

            let scatter =
                Scatter.lastOccurrence clContext workGroupSize

            fun (processor: MailboxProcessor<_>) allocationMode (vector: ClVector.Sparse<'a>) ->

                let bitmap =
                    getOptionBitmap processor DeviceOnly vector.Values

                let resultLength =
                    (prefixSum processor bitmap)
                        .ToHostAndFree processor

                ()
