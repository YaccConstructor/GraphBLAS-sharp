namespace GraphBLAS.FSharp.Backend.Common

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic

module internal rec Copy =
    let copyArray (inputArray: 'a[]) =
        if inputArray.Length = 0 then
            opencl { return [||] }
        else
            copyNonEmpty inputArray

    let private copyNonEmpty (inputArray: 'a[]) = opencl {
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
            let ndRange = _1D(Utils.getDefaultGlobalSize inputArray.Length, Utils.defaultWorkGroupSize)
            kernelPrepare ndRange inputArray outputArray

        return outputArray
    }
