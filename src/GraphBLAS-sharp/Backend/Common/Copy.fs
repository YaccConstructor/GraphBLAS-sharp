namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp.OpenCL

module internal Copy =
    let private copyNonEmpty (inputArray: 'a []) =
        opencl {
            let inputArrayLength = inputArray.Length

            let copy =
                <@ fun (ndRange: Range1D) (inputArrayBuffer: 'a []) (outputArrayBuffer: 'a []) ->

                    let i = ndRange.GlobalID0

                    if i < inputArrayLength then
                        outputArrayBuffer.[i] <- inputArrayBuffer.[i] @>

            let outputArray = Array.zeroCreate inputArray.Length

            do!
                runCommand copy
                <| fun kernelPrepare ->
                    let ndRange =
                        Range1D(Utils.getDefaultGlobalSize inputArray.Length, Utils.defaultWorkGroupSize)

                    kernelPrepare ndRange inputArray outputArray

            return outputArray
        }


    let copyArray (inputArray: 'a []) =
        if inputArray.Length = 0 then
            opencl { return [||] }
        else
            copyNonEmpty inputArray
