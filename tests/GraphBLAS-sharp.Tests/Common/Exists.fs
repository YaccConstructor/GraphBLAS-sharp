module GraphBLAS.FSharp.Tests.Backend.Common.Exists

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Utils
open Context
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Quotes

let logger =
    Log.create "ClArray.containsNonZero.Tests"

let context = defaultContext.ClContext

let q = defaultContext.Queue

let correctnessGenericTest<'a when 'a: struct and 'a: equality> isZero exists (array: 'a []) =

    if array.Length > 0 then
        let vector = createVectorFromArray Dense array isZero

        let result =
            match vector.ToDevice context with
            | ClVector.Dense clArray ->
                let resultCell = exists q clArray
                let result = Array.zeroCreate 1

                let res =
                    q.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(resultCell, result, ch))

                q.Post(Msg.CreateFreeMsg<_>(resultCell))

                res.[0]

            | _ -> failwith "Unsupported vector format"

        $"The results should be the same, vector : {vector}"
        |> Expect.equal result (Array.exists (not << isZero) array)

let testFixtures =
    let config = defaultConfig

    let wgSize = 32

    let getCorrectnessTestName datatype =
        sprintf "Correctness on %s, %A" datatype Dense

    [ let exists =
          ClArray.exists context wgSize Predicates.isSome

      correctnessGenericTest<int> ((=) 0) exists
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      correctnessGenericTest<int> ((=) 0) exists (Array.create 1000 0)
      |> testPropertyWithConfig config (getCorrectnessTestName "int zeros")

      let exists =
          ClArray.exists context wgSize Predicates.isSome

      correctnessGenericTest<byte> ((=) 0uy) exists
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")

      correctnessGenericTest<byte> ((=) 0uy) exists (Array.create 1000 0uy)
      |> testPropertyWithConfig config (getCorrectnessTestName "byte zeros")

      let exists =
          ClArray.exists context wgSize Predicates.isSome

      correctnessGenericTest<float> ((=) 0.0) exists
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      correctnessGenericTest<float> ((=) 0.0) exists (Array.create 1000 0.0)
      |> testPropertyWithConfig config (getCorrectnessTestName "float zeros")

      let exists =
          ClArray.exists context wgSize Predicates.isSome

      correctnessGenericTest<bool> ((=) false) exists
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      correctnessGenericTest<bool> ((=) false) exists (Array.create 1000 false)
      |> testPropertyWithConfig config (getCorrectnessTestName "bool zeros") ]

let tests =
    testList "Backend.Vector.containsNonZero tests" testFixtures
