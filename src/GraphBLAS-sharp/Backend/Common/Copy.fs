namespace GraphBLAS.FSharp.Backend.Common

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Utils

module internal Copy =
    let runNotEmpty (inputArray: 'a[]) = opencl {
        let inputArrayLength = inputArray.Length
        let copy =
            <@
                fun (ndRange: _1D)
                    (inputArrayBuffer: 'a[])
                    (outputArrayBuffer: 'a[]) ->

                    let i = ndRange.GlobalID0
                    if i < inputArrayLength then
                        outputArrayBuffer.[i] <- inputArrayBuffer.[i]
            @>

        let outputArray = Array.zeroCreate inputArray.Length

        do! RunCommand copy <| fun kernelPrepare ->
            let ndRange = _1D(workSize inputArray.Length, workGroupSize)
            kernelPrepare ndRange inputArray outputArray
        return outputArray
    }

    let run (inputArray: 'a[]) = if inputArray.Length = 0 then opencl { return [||] } else runNotEmpty inputArray
