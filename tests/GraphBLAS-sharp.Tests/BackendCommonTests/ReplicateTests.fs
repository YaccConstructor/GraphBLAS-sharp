module BackendTests.Replicate

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests

let logger = Log.create "Replicate.Tests"

let context = Utils.defaultContext.ClContext

let testCases =
    let q = Utils.defaultContext.Queue

    let getReplicateFun replicate =
        fun (array: array<_>) ->
            let wgSize =
                [| for i in 0 .. 5 -> pown 2 i |]
                |> Array.filter (fun i -> array.Length % i = 0)
                |> Array.max

            replicate q wgSize

    let makeTest getReplicateFun (array: array<'a>) filterFun i =
        if array.Length > 0 && i > 0 then
            use clArray = context.CreateClArray array

            let replicate = getReplicateFun array

            let actual =
                use clActual: ClArray<'a> = replicate clArray i

                let actual = Array.zeroCreate clActual.Length
                q.PostAndReply(fun ch -> Msg.CreateToHostMsg(clActual, actual, ch))

            logger.debug (eventX "Actual is {actual}" >> setField "actual" $"%A{actual}")

            let expected = array |> Array.replicate i |> Array.concat |> filterFun

            let actual = filterFun actual

            $"Array should contains %i{i} copies of the original one"
            |> Expect.sequenceEqual actual expected

    [
        let replicateInt = getReplicateFun <| ClArray.replicate context
        testProperty "Correctness test on random int arrays" <| fun (array: array<int>) ->
            makeTest replicateInt array id

        let replicateBool = getReplicateFun <| ClArray.replicate context
        testProperty "Correctness test on random bool arrays" <| fun (array: array<bool>) ->
            makeTest replicateBool array id

        let replicateFloat = getReplicateFun <| ClArray.replicate context
        testProperty "Correctness test on random float arrays" <| fun (array: array<float>) ->
            makeTest replicateFloat array (Array.filter (System.Double.IsNaN >> not))

        let replicateByte = getReplicateFun <| ClArray.replicate context
        testProperty "Correctness test on random byte arrays" <| fun (array: array<byte>) ->
            makeTest replicateByte array id
    ]

let tests =
    testCases
    |> testList "Array.replicate tests"
