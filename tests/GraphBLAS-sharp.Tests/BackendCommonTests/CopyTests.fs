module Backend.Copy

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend

let logger = Log.create "Copy.Tests"

let context =
    let deviceType = ClDeviceType.Default
    let platformName = ClPlatform.Any
    ClContext(platformName, deviceType)

let testCases =
    let q = context.Provider.CommandQueue
    q.Error.Add(fun e -> failwithf "%A" e)

    let getCopyFun copy =
        fun (array: array<_>) ->
            let wgSize =
                [| for i in 0 .. 5 -> pown 2 i |]
                |> Array.filter (fun i -> array.Length % i = 0)
                |> Array.max

            copy q wgSize

    let makeTest getCopyFun (array: array<'a>) filterFun =
        if array.Length > 0 then
            use clArray = context.CreateClArray array

            let copy = getCopyFun array

            let actual =
                use clActual: ClArray<'a> = copy clArray

                let actual = Array.zeroCreate clActual.Length
                q.PostAndReply(fun ch -> Msg.CreateToHostMsg(clActual, actual, ch))

            logger.debug (
                eventX "Actual is {actual}"
                >> setField "actual" (sprintf "%A" actual)
            )

            let expected = filterFun array
            let actual = filterFun actual

            "Array should be equals to original"
            |> Expect.sequenceEqual actual expected

    [ testProperty "Correctness test on random int arrays"
      <| (let copy = ClArray.copy context
          let getCopyFun = getCopyFun copy
          fun (array: array<int>) -> makeTest getCopyFun array id)

      testProperty "Correctness test on random bool arrays"
      <| (let copy = ClArray.copy context
          let getCopyFun = getCopyFun copy

          fun (array: array<bool>) -> makeTest getCopyFun array id)

      testProperty "Correctness test on random float arrays"
      <| (let copy = ClArray.copy context
          let getCopyFun = getCopyFun copy

          fun (array: array<float>) -> makeTest getCopyFun array (Array.filter (System.Double.IsNaN >> not)))

      testProperty "Correctness test on random byte arrays"
      <| (let copy = ClArray.copy context
          let getCopyFun = getCopyFun copy

          fun (array: array<byte>) -> makeTest getCopyFun array id)

      ]

let tests = testCases |> testList "Array.copy tests"
