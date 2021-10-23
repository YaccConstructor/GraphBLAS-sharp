module Backend.Replicate

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend

let logger = Log.create "Replicate.Tests"

let context =
    let deviceType = ClDeviceType.Default
    let platformName = ClPlatform.Any
    ClContext(platformName, deviceType)

let testCases =
    let q = context.Provider.CommandQueue
    q.Error.Add(fun e -> failwithf "%A" e)

    let getReplicateFun replicate =
        fun (array: array<_>) ->
            let wgSize =
                [| for i in 0 .. 5 -> pown 2 i |]
                |> Array.filter (fun i -> array.Length % i = 0)
                |> Array.max

            replicate q wgSize

    let makeTest getReplicateFun (array: array<'a>) i =
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
                array |> Array.replicate i |> Array.concat

            sprintf "Array should contains %i copies of the original one" i
            |> Expect.sequenceEqual actual expected

    [ testProperty "Correctness test on random int arrays"
      <| (let replicate = ClArray.replicate context
          let getReplicateFun = getReplicateFun replicate
          fun (array: array<int>) -> makeTest getReplicateFun array)

      testProperty "Correctness test on random bool arrays"
      <| (let replicate = ClArray.replicate context
          let getReplicateFun = getReplicateFun replicate

          fun (array: array<bool>) -> makeTest getReplicateFun array)

      testProperty "Correctness test on random float arrays"
      <| (let replicate = ClArray.replicate context
          let getReplicateFun = getReplicateFun replicate

          fun (array: array<float>) -> makeTest getReplicateFun array)

      testProperty "Correctness test on random byte arrays"
      <| (let replicate = ClArray.replicate context
          let getReplicateFun = getReplicateFun replicate

          fun (array: array<byte>) -> makeTest getReplicateFun array)

      ]

let tests =
    testCases |> testList "Array.replicate tests"
