namespace GraphBLAS.FSharp.Backend.Common

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open Utils

module internal Copy =
    let runNotEmpty (inputArray: 'a[]) =
        let outputArray = Array.zeroCreate inputArray.Length
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
        let binder kernelP =
            let ndRange = _1D(workSize inputArray.Length, workGroupSize)
            kernelP ndRange inputArray outputArray
        opencl {
            do! RunCommand copy binder
            return outputArray
        }

    let run (inputArray: 'a[]) = if inputArray.Length = 0 then opencl { return [||] } else runNotEmpty inputArray
