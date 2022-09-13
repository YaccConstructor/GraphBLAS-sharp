module BackendTests.Copy

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests

let logger = Log.create "Copy.Tests"

let context = Utils.defaultContext.ClContext

let testCases =
    let q = Utils.defaultContext.Queue

    let getCopyFun copy =
        fun (array: array<_>) ->
            let wgSize =
                [| for i in 0 .. 5 -> pown 2 i |]
                |> Array.filter (fun i -> array.Length % i = 0)
                |> Array.max

            copy wgSize q

    let makeTest getCopyFun (array: array<'a>) filterFun =
        if array.Length > 0 then
            use clArray = context.CreateClArray array
            let copy = getCopyFun array

            let actual =
                use clActual: ClArray<'a> = copy clArray

                let actual = Array.zeroCreate clActual.Length
                q.PostAndReply(fun ch -> Msg.CreateToHostMsg(clActual, actual, ch))

            logger.debug (eventX "Actual is {actual}" >> setField "actual" (sprintf "%A" actual))

            let expected = filterFun array
            let actual = filterFun actual

            "Array should be equals to original" |> Expect.sequenceEqual actual expected

    [
        let copyInt = getCopyFun <| ClArray.copy context
        testProperty "Correctness test on random int arrays" <| fun (array: array<int>) ->
            makeTest copyInt array id

        let copyBool = getCopyFun <| ClArray.copy context
        testProperty "Correctness test on random bool arrays" <| fun (array: array<bool>) ->
            makeTest copyBool array id

        let copyFloat = getCopyFun <| ClArray.copy context
        testProperty "Correctness test on random float arrays" <| fun (array: array<float>) ->
            makeTest copyFloat array (Array.filter (System.Double.IsNaN >> not))

        let copyByte = getCopyFun <| ClArray.copy context
        testProperty "Correctness test on random byte arrays" <| fun (array: array<byte>) ->
            makeTest copyByte array id
    ]

let tests =
    testCases
    |> testList "Array.copy tests"
