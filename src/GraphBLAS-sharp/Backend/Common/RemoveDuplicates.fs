namespace GraphBLAS.FSharp.Backend.Common

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic

module internal RemoveDuplicates =
    let run (array: 'a[]) = opencl {
        let inputLength = array.Length

        let isUniqueBitmap =
            <@
                fun (ndRange: _1D)
                    (inputArray: 'a[])
                    (isUniqueBitmap: int[]) ->

                    let i = ndRange.GlobalID0
                    if i < inputLength - 1 && inputArray.[i] = inputArray.[i + 1] then
                        isUniqueBitmap.[i] <- 0
            @>

        let setPositions =
            <@
                fun (ndRange: _1D)
                    (inputArray: 'a[])
                    (positions: int[])
                    (ouputArray: 'a[]) ->

                    let i = ndRange.GlobalID0
                    if i < inputLength then
                        let position = positions.[i] - 1
                        ouputArray.[position] <- inputArray.[i]
            @>

        let bitmap = Array.create inputLength 1
        do! RunCommand isUniqueBitmap <| fun kernelPrepare ->
            let ndRange = _1D(Utils.workSize inputLength, Utils.workGroupSize)
            kernelPrepare ndRange array bitmap

        let resultLength = Array.zeroCreate 1
        do! PrefixSum.runInplace bitmap resultLength
        let! _ = ToHost resultLength
        let resultLength = resultLength.[0]

        let outputArray = Array.zeroCreate resultLength
        do! RunCommand setPositions <| fun kernelPrepare ->
            let ndRange = _1D(Utils.workSize inputLength, Utils.workGroupSize)
            kernelPrepare ndRange array bitmap outputArray

        return outputArray
    }
