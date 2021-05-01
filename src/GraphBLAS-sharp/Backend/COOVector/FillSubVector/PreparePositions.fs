namespace GraphBLAS.FSharp.Backend.COOVector.FillSubVector

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp.Backend.Common

[<AutoOpen>]
module internal PreparePositions =
    let preparePositions (allIndices: int[]) : OpenCLEvaluation<int[]> = opencl {
        let length = allIndices.Length

        let preparePositions =
            <@
                fun (ndRange: _1D)
                    (allIndicesBuffer: int[])
                    (rawPositionsBuffer: int[]) ->

                    let i = ndRange.GlobalID0

                    if i < length - 1 && allIndicesBuffer.[i] = allIndicesBuffer.[i + 1] then rawPositionsBuffer.[i] <- 0
            @>

        let rawPositions = Array.create length 1

        do! RunCommand preparePositions <| fun kernelPrepare ->
            let ndRange = _1D(Utils.workSize (length - 1), Utils.workGroupSize)
            kernelPrepare
                ndRange
                allIndices
                rawPositions

        return rawPositions
    }
