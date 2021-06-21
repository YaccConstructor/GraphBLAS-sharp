namespace GraphBLAS.FSharp.Backend.COOVector.Utilities

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp.Backend.Common

[<AutoOpen>]
module internal SetPositions =
    let setPositions (allIndices: int[]) (allValues: 'a[]) (positions: int[]) : OpenCLEvaluation<int[] * 'a[]> = opencl {
        let prefixSumArrayLength = positions.Length

        let setPositions =
            <@
                fun (ndRange: _1D)
                    (allIndicesBuffer: int[])
                    (allValuesBuffer: 'a[])
                    (prefixSumArrayBuffer: int[])
                    (resultIndicesBuffer: int[])
                    (resultValuesBuffer: 'a[]) ->

                    let i = ndRange.GlobalID0

                    if i = prefixSumArrayLength - 1 || i < prefixSumArrayLength && prefixSumArrayBuffer.[i] <> prefixSumArrayBuffer.[i + 1] then
                        let index = prefixSumArrayBuffer.[i]

                        resultIndicesBuffer.[index] <- allIndicesBuffer.[i]
                        resultValuesBuffer.[index] <- allValuesBuffer.[i]
            @>

        let resultLength = Array.zeroCreate 1

        do! PrefixSum.runExcludeInplace positions resultLength
        let! _ = ToHost resultLength
        let resultLength = resultLength.[0]

        let resultIndices = Array.zeroCreate resultLength
        let resultValues = Array.create resultLength Unchecked.defaultof<'a>

        do! RunCommand setPositions <| fun kernelPrepare ->
            let ndRange = _1D(Utils.getDefaultGlobalSize positions.Length, Utils.defaultWorkGroupSize)
            kernelPrepare
                ndRange
                allIndices
                allValues
                positions
                resultIndices
                resultValues

        return resultIndices, resultValues
    }
