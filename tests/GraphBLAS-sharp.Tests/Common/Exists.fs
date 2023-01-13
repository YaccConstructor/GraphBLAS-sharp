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
    Log.create "Vector.containsNonZero.Tests"

let context = defaultContext.ClContext

let q = defaultContext.Queue

let correctnessGenericTest<'a when 'a: struct and 'a: equality> isZero containsNonZero (array: 'a []) =

    if array.Length > 0 then
        let vector = createVectorFromArray Dense array isZero

        let result =
            match vector.ToDevice context with
            | ClVector.Dense clArray ->
                let resultCell = containsNonZero q clArray
                let result = Array.zeroCreate 1

                let res =
                    q.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(resultCell, result, ch))

                q.Post(Msg.CreateFreeMsg<_>(resultCell))

                res.[0]

        $"The results should be the same, vector : {vector}"
        |> Expect.equal result (Array.exists (not << isZero) array)

let testFixtures =
    let config = defaultConfig

    let wgSize = 32

    let getCorrectnessTestName datatype =
        sprintf "Correctness on %s, %A" datatype Dense

    [ let containsNonZeroInt =
          ClArray.exists context wgSize Predicates.containsNonZero

      correctnessGenericTest<int> ((=) 0) containsNonZeroInt
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let containsNonZeroByte =
          ClArray.exists context wgSize Predicates.containsNonZero

      correctnessGenericTest<byte> ((=) 0uy) containsNonZeroByte
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")

      let containsNonZeroFloat =
          ClArray.exists context wgSize Predicates.containsNonZero

      correctnessGenericTest<float> ((=) 0.0) containsNonZeroFloat
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let containsNonZeroBool =
          ClArray.exists context wgSize Predicates.containsNonZero

      correctnessGenericTest<bool> ((=) false) containsNonZeroBool
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      let containsNonZeroInt =
          ClArray.exists context wgSize Predicates.containsNonZero

      correctnessGenericTest<int> ((=) 0) containsNonZeroInt (Array.create 1000 0)
      |> testPropertyWithConfig config (getCorrectnessTestName "int zeros")

      let containsNonZeroByte =
          ClArray.exists context wgSize Predicates.containsNonZero

      correctnessGenericTest<byte> ((=) 0uy) containsNonZeroByte (Array.create 1000 0uy)
      |> testPropertyWithConfig config (getCorrectnessTestName "byte zeros")

      let containsNonZeroFloat =
          ClArray.exists context wgSize Predicates.containsNonZero

      correctnessGenericTest<float> ((=) 0.0) containsNonZeroFloat (Array.create 1000 0.0)
      |> testPropertyWithConfig config (getCorrectnessTestName "float zeros")

      let containsNonZeroBool =
          ClArray.exists context wgSize Predicates.containsNonZero

      correctnessGenericTest<bool> ((=) false) containsNonZeroBool (Array.create 1000 false)
      |> testPropertyWithConfig config (getCorrectnessTestName "bool zeros") ]

let tests =
    testList "Backend.Vector.containsNonZero tests" testFixtures
