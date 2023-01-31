module GraphBLAS.FSharp.Tests.Backend.Common.Replicate

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects.ClContext

let logger = Log.create "Replicate.Tests"

let context = Context.defaultContext.ClContext

let testCases =
    let q = Context.defaultContext.Queue
    q.Error.Add(fun e -> failwithf "%A" e)

    let getReplicateFun replicate =
        fun (array: array<_>) ->
            let wgSize =
                [| for i in 0 .. 5 -> pown 2 i |]
                |> Array.filter (fun i -> array.Length % i = 0)
                |> Array.max

            replicate wgSize q HostInterop

    let makeTest getReplicateFun (array: array<'a>) filterFun i =
        if array.Length > 0 && i > 0 then
            use clArray = context.CreateClArray array

            let replicate = getReplicateFun array

            let actual =
                use clActual: ClArray<'a> = replicate clArray i

                let actual = Array.zeroCreate clActual.Length
                q.PostAndReply(fun ch -> Msg.CreateToHostMsg(clActual, actual, ch))

            logger.debug (
                eventX "Actual is {actual}"
                >> setField "actual" (sprintf "%A" actual)
            )

            let expected =
                array
                |> Array.replicate i
                |> Array.concat
                |> filterFun

            let actual = filterFun actual

            sprintf "Array should contains %i copies of the original one" i
            |> Expect.sequenceEqual actual expected

    [ testProperty "Correctness test on random int arrays"
      <| (let replicate = ClArray.replicate context
          let getReplicateFun = getReplicateFun replicate
          fun (array: array<int>) -> makeTest getReplicateFun array id)

      testProperty "Correctness test on random bool arrays"
      <| (let replicate = ClArray.replicate context
          let getReplicateFun = getReplicateFun replicate

          fun (array: array<bool>) -> makeTest getReplicateFun array id)

      testProperty "Correctness test on random float arrays"
      <| (let replicate = ClArray.replicate context
          let getReplicateFun = getReplicateFun replicate

          fun (array: array<float>) -> makeTest getReplicateFun array (Array.filter (System.Double.IsNaN >> not)))

      testProperty "Correctness test on random byte arrays"
      <| (let replicate = ClArray.replicate context
          let getReplicateFun = getReplicateFun replicate

          fun (array: array<byte>) -> makeTest getReplicateFun array id)

      ]

let tests =
    testCases |> testList "Array.replicate tests"
