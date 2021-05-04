module Backend.BitonicSort

open Expecto
open FsCheck
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Predefined
open TypeShape.Core
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open OpenCL.Net
open GraphBLAS.FSharp.Backend.Common

let logger = Log.create "BitonicSortTests"

let testCases = [
    let config = Utils.defaultConfig

    testPropertyWithConfig config "Simple correctness test on int" <| fun (array: int[]) ->
        let sortedArray =
            opencl {
                let copiedArray = Array.copy array
                do! BitonicSort.sortInplace copiedArray
                let! _ = ToHost copiedArray
                return copiedArray
            }
            |> OpenCLEvaluationContext().RunSync

        "Array should be sorted by ascending"
        |> Expect.isAscending sortedArray

    testPropertyWithConfig config "Simple correctness test on uint64" <| fun (array: uint64[]) ->
        let sortedArray =
            opencl {
                let copiedArray = Array.copy array
                do! BitonicSort.sortInplace copiedArray
                let! _ = ToHost copiedArray
                return copiedArray
            }
            |> OpenCLEvaluationContext().RunSync

        "Array should be sorted by ascending"
        |> Expect.isAscending sortedArray

    testPropertyWithConfig config "Simple correctness test on float32" <| fun (array: float32[]) ->
        let sortedArray =
            opencl {
                let copiedArray = Array.copy array
                do! BitonicSort.sortInplace copiedArray
                let! _ = ToHost copiedArray
                return copiedArray
            }
            |> OpenCLEvaluationContext().RunSync

        "Array should be sorted by ascending"
        |> Expect.isAscending sortedArray
]

let tests =
    testCases
    |> testList "Bitonic sort tests"
