namespace GraphBLAS.FSharp.Backend.Common

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic

module internal rec Replicate =
    let run (count: int) (inputArray: 'a[]) =
        if count < 0 then
            invalidArg "count" "Value cannot be less than 0"
        elif inputArray.Length = 0 || count = 0 then
            opencl { return [||] }
        else
            runNotEmpty count inputArray

    let private runNotEmpty (count: int) (inputArray: 'a[]) = opencl {
        let inputArrayLength = inputArray.Length
        let outputArrayLength = inputArrayLength * count
        let replicate =
            <@
                fun (ndRange: _1D)
                    (inputArrayBuffer: 'a[])
                    (outputArrayBuffer: 'a[]) ->

                    let i = ndRange.GlobalID0
                    if i < outputArrayLength then
                        outputArrayBuffer.[i] <- inputArrayBuffer.[i % inputArrayLength]
            @>

        let outputArray = Array.zeroCreate outputArrayLength

        do! RunCommand replicate <| fun kernelPrepare ->
            let ndRange = _1D(Utils.workSize outputArray.Length, Utils.workGroupSize)
            kernelPrepare ndRange inputArray outputArray
        return outputArray
    }
