namespace GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic

module internal Toolbox =
    let workGroupSize = 128
    let workSize n =
        let m = n - 1
        m - m % workGroupSize + workGroupSize

    let rec prefixSum (inputArray: int[]) =
        let outputArray = Array.zeroCreate inputArray.Length

        if inputArray.Length = 1 then
            let fillOutputArray =
                <@
                    fun (ndRange: _1D)
                        (inputArrayBuffer: int[])
                        (outputArrayBuffer: int[]) ->

                        let i = ndRange.GlobalID0
                        outputArrayBuffer.[i] <- inputArrayBuffer.[i]
                @>

            opencl {
                let binder kernelP =
                    let ndRange = _1D(outputArray.Length)
                    kernelP
                        ndRange
                        inputArray
                        outputArray
                do! RunCommand fillOutputArray binder
                return outputArray
            }
        else
            let intermediateArray = Array.zeroCreate ((inputArray.Length + 1) / 2)
            let inputArrayLength = inputArray.Length
            let intermediateArrayLength = intermediateArray.Length

            let fillIntermediateArray =
                <@
                    fun (ndRange: _1D)
                        (inputArrayBuffer: int[])
                        (intermediateArrayBuffer: int[]) ->

                        let i = ndRange.GlobalID0
                        if i < intermediateArrayLength then
                            if 2 * i + 1 < inputArrayLength then
                                intermediateArrayBuffer.[i] <- inputArrayBuffer.[2 * i] + inputArrayBuffer.[2 * i + 1]
                            else intermediateArrayBuffer.[i] <- inputArrayBuffer.[2 * i]
                @>

            let fillIntermediateArray =
                opencl {
                    let binder kernelP =
                        let ndRange = _1D(workSize intermediateArray.Length, workGroupSize)
                        kernelP
                            ndRange
                            inputArray
                            intermediateArray
                    do! RunCommand fillIntermediateArray binder
                }

            let fillOutputArray =
                <@
                    fun (ndRange: _1D)
                        (auxiliaryPrefixSumArrayBuffer: int[])
                        (inputArrayBuffer: int[])
                        (outputArrayBuffer: int[]) ->

                        let i = ndRange.GlobalID0
                        if i < inputArrayLength then
                            let j = (i - 1) / 2
                            if i % 2 = 0 then
                                if i = 0 then outputArrayBuffer.[i] <- inputArrayBuffer.[i]
                                else outputArrayBuffer.[i] <- auxiliaryPrefixSumArrayBuffer.[j] + inputArrayBuffer.[i]
                            else outputArrayBuffer.[i] <- auxiliaryPrefixSumArrayBuffer.[j]
                @>

            opencl {
                do! fillIntermediateArray
                let! auxiliaryPrefixSumArray = prefixSum intermediateArray

                let binder kernelP =
                    let ndRange = _1D(workSize inputArray.Length, workGroupSize)
                    kernelP
                        ndRange
                        auxiliaryPrefixSumArray
                        inputArray
                        outputArray
                do! RunCommand fillOutputArray binder

                return outputArray
            }

    let prefixSum2 (inputArray: int[]) =
        let firstIntermediateArray = Array.copy inputArray
        let secondIntermediateArray = Array.copy inputArray
        let outputArrayLength = firstIntermediateArray.Length

        let updateResult =
            <@
                fun (ndRange: _1D)
                    (offset: int)
                    (firstIntermediateArrayBuffer: int[])
                    (secondIntermediateArrayBuffer: int[]) ->

                    let i = ndRange.GlobalID0
                    if i < outputArrayLength then
                        if i < offset then firstIntermediateArrayBuffer.[i] <- secondIntermediateArrayBuffer.[i]
                        else firstIntermediateArrayBuffer.[i] <- secondIntermediateArrayBuffer.[i] + secondIntermediateArrayBuffer.[i - offset]
            @>

        let binder offset firstIntermediateArray secondIntermediateArray kernelP =
            let ndRange = _1D(workSize outputArrayLength, workGroupSize)
            kernelP
                ndRange
                offset
                firstIntermediateArray
                secondIntermediateArray

        let swap (a, b) = (b, a)
        let mutable arrays = firstIntermediateArray, secondIntermediateArray

        opencl {
            let mutable offset = 1
            while offset < outputArrayLength do
                arrays <- swap arrays
                do! RunCommand updateResult <| (binder offset <|| arrays)
                offset <- offset * 2

            return (fst arrays)
        }
