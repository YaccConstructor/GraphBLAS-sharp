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

let logger = Log.create "BitonicSort.Tests"

let testCases = [
    let config = Utils.defaultConfig

    testPropertyWithConfig config "Simple correctness test on int" <| fun (array: int[]) ->
        let expected = Array.sort array

        let actual =
            opencl {
                let copiedArray = Array.copy array
                do! BitonicSort.sortInplace3 copiedArray
                if array.Length <> 0 then
                    let! _ = ToHost copiedArray
                    ()
                return copiedArray
            }
            |> OpenCLEvaluationContext().RunSync

        "Actual array should be equal to sorted"
        |> Expect.sequenceEqual actual expected

    // testCase "Simple correctness test on int" <| fun () ->
    //     let array = Array.init 600 (fun i -> 600 - i)
    //     let expected = Array.sort array

    //     let actual =
    //         opencl {
    //             let copiedArray = Array.copy array
    //             do! BitonicSort.sortInplace3 copiedArray
    //             let! _ = ToHost copiedArray
    //             return copiedArray
    //         }
    //         |> OpenCLEvaluationContext().RunSync

    //     "Actual array should be equal to sorted"
    //     |> Expect.sequenceEqual actual expected

    // testPropertyWithConfig config "Simple correctness test on uint64" <| fun (array: uint64[]) ->
    //     let expected = Array.sort array

    //     let actual =
    //         opencl {
    //             let copiedArray = Array.copy array
    //             do! BitonicSort.sortInplace copiedArray
    //             if array.Length <> 0 then
    //                 let! _ = ToHost copiedArray
    //                 ()
    //             return copiedArray
    //         }
    //         |> OpenCLEvaluationContext().RunSync

    //     "Actual array should be equal to sorted"
    //     |> Expect.sequenceEqual actual expected

    ftestPropertyWithConfig config "Simple correctness test on uint64 * int" <| fun (array: (uint64*int)[]) ->
        let expected = Array.sortBy (fun (key, value) -> key) array

        let (a, b) = Array.unzip array

        let actual =
            opencl {
                do! BitonicSort.sortInplace2 a b
                if array.Length <> 0 then
                    let! _ = ToHost a
                    let! _ = ToHost b
                    ()

                return a, b
            }
            |> OpenCLEvaluationContext().RunSync
            ||> Array.zip

        "Actual array should be equal to sorted"
        |> Expect.sequenceEqual actual expected
]

let tests =
    testCases
    |> testList "BitonicSort tests"
