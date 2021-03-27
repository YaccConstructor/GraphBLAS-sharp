namespace GraphBLAS.FSharp.Backend.COOVector.Utilities

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Quotations

[<AutoOpen>]
module internal PreparePositions =
    let preparePositions (allIndices: int[]) (allValues: 'a[]) (plus: Expr<'a -> 'a -> 'a>) : OpenCLEvaluation<int[]> = opencl {
        let length = allValues.Length

        let preparePositions =
            <@
                fun (ndRange: _1D)
                    (allIndicesBuffer: int[])
                    (allValuesBuffer: 'a[])
                    (rawPositionsBuffer: int[]) ->

                    let i = ndRange.GlobalID0

                    if i < length - 1 && allIndicesBuffer.[i] = allIndicesBuffer.[i + 1] then
                        rawPositionsBuffer.[i] <- 0

                        //Do not drop explicit zeroes
                        allValuesBuffer.[i + 1] <- (%plus) allValuesBuffer.[i] allValuesBuffer.[i + 1]

                        //Drop explicit zeroes
                        // let localResultBuffer = (%plus) allValuesBuffer.[i] allValuesBuffer.[i + 1]
                        // if localResultBuffer = zero then rawPositionsBuffer.[i + 1] <- 0 else allValuesBuffer.[i + 1] <- localResultBuffer
            @>

        let rawPositions = Array.create length 1

        do! RunCommand preparePositions <| fun kernelPrepare ->
            let ndRange = _1D(Utils.workSize (length - 1), Utils.workGroupSize)
            kernelPrepare
                ndRange
                allIndices
                allValues
                rawPositions

        return rawPositions
    }
