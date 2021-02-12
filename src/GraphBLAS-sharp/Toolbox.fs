namespace GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions
open GlobalContext
open Helpers
open FSharp.Quotations.Evaluator
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

module internal Toolbox =

    let internal workGroupSize = 128
    let internal workSize n =
        let m = n - 1
        m - m % workGroupSize + workGroupSize

    let rec internal prefixSum
        (inputArray: int[]) =

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

    let internal prefixSum2
        (inputArray: int[]) =

        let outputArray = Array.copy inputArray
        let auxiliaryArray = Array.copy inputArray
        let outputArrayLength = outputArray.Length

        let updateResult =
            <@
                fun (ndRange: _1D)
                    (offset: int)
                    (outputArrayBuffer: int[])
                    (auxiliaryArrayBuffer: int[]) ->

                    let i = ndRange.GlobalID0
                    if i < outputArrayLength then
                        if i < offset then outputArrayBuffer.[i] <- auxiliaryArrayBuffer.[i]
                        else outputArrayBuffer.[i] <- auxiliaryArrayBuffer.[i] + auxiliaryArrayBuffer.[i - offset]
            @>

        let binder offset outputArray auxiliaryArray kernelP =
            let ndRange = _1D(workSize outputArrayLength, workGroupSize)
            kernelP
                ndRange
                offset
                outputArray
                auxiliaryArray

        let swap (a, b) = (b, a)
        let mutable arrays = outputArray, auxiliaryArray

        opencl {

            let mutable offset = 1
            while offset < outputArrayLength do
                arrays <- swap arrays
                do! RunCommand updateResult <| (binder offset <|| arrays)
                offset <- offset * 2

            return (fst arrays)
        }

    module internal EWiseAdd =
        let internal dropExplicitZeroes
            (zero: 'a)
            (allValues: 'a[])
            (auxiliaryArray: int[]) =

            let command =
                <@
                    fun (ndRange: _1D)
                        (allValuesBuffer: 'a[])
                        (auxiliaryArrayBuffer: int[]) ->

                        let i = ndRange.GlobalID0
                        if allValuesBuffer.[i] = zero then auxiliaryArrayBuffer.[i] <- 0
                @>

            let binder kernelP =
                let ndRange = _1D(allValues.Length)
                kernelP
                    ndRange
                    allValues
                    auxiliaryArray

            opencl {
                do! RunCommand command binder
            }
