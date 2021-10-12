namespace GraphBLAS.FSharp.Backend.Common

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic

module internal RemoveDuplicates =
    let fromArray (array: 'a []) =
        opencl {
            if array.Length = 0 then
                return [||]

            else
                let inputLength = array.Length

                let getUniqueBitmap =
                    <@ fun (ndRange: _1D) (inputArray: 'a []) (isUniqueBitmap: int []) ->

                        let i = ndRange.GlobalID0

                        if i < inputLength - 1
                           && inputArray.[i] = inputArray.[i + 1] then
                            isUniqueBitmap.[i] <- 0 @>

                let setPositions =
                    <@ fun (ndRange: _1D) (inputArray: 'a []) (positions: int []) (outputArray: 'a []) ->

                        let i = ndRange.GlobalID0

                        if i < inputLength then
                            outputArray.[positions.[i]] <- inputArray.[i] @>

                let bitmap = Array.create inputLength 1

                do!
                    RunCommand getUniqueBitmap
                    <| fun kernelPrepare ->
                        let range =
                            _1D (Utils.getDefaultGlobalSize inputLength, Utils.defaultWorkGroupSize)

                        kernelPrepare range array bitmap

                let! (positions, sum) = PrefixSum.runExclude bitmap
                let! _ = ToHost sum
                let resultLength = sum.[0]

                let outputArray = Array.zeroCreate resultLength

                do!
                    RunCommand setPositions
                    <| fun kernelPrepare ->
                        let range =
                            _1D (Utils.getDefaultGlobalSize inputLength, Utils.defaultWorkGroupSize)

                        kernelPrepare range array positions outputArray

                return outputArray
        }
