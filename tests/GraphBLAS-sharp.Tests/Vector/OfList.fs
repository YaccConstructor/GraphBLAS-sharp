module GraphBLAS.FSharp.Tests.Backend.Vector.OfList

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Utils
open GraphBLAS.FSharp.Backend
open Context
open TestCases
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

let logger = Log.create "Vector.ofList.Tests"

let checkResult
    (isEqual: 'a -> 'a -> bool)
    (expectedIndices: int [])
    (expectedValues: 'a [])
    (actual: Vector<'a>)
    actualSize
    =

    Expect.equal actual.Size actualSize "lengths must be the same"

    match actual with
    | Vector.Sparse actual ->
        compareArrays (=) actual.Indices expectedIndices "indices must be the same"
        compareArrays isEqual actual.Values expectedValues "values must be the same"
    | _ -> failwith "Vector format must be Sparse."

let correctnessGenericTest<'a when 'a: struct>
    (isEqual: 'a -> 'a -> bool)
    (ofList: MailboxProcessor<_> -> AllocationFlag -> VectorFormat -> int -> (int * 'a) list -> ClVector<'a>)
    (toCoo: MailboxProcessor<_> -> AllocationFlag -> ClVector<'a> -> ClVector<'a>)
    (case: OperationCase<VectorFormat>)
    (elements: (int * 'a) [])
    (sizeDelta: int)
    =

    let elements =
        elements |> Array.distinctBy fst |> List.ofArray

    if elements.Length > 0 then

        let q = case.TestContext.Queue

        let indices, values =
            elements
            |> Array.ofList
            |> Array.sortBy fst
            |> Array.unzip

        let actualSize = (Array.max indices) + abs sizeDelta + 1

        let clActual =
            ofList q HostInterop case.Format actualSize elements

        let clCooActual = toCoo q HostInterop clActual

        let actual = clCooActual.ToHost q

        clActual.Dispose q
        clCooActual.Dispose q

        checkResult isEqual indices values actual actualSize

let testFixtures (case: OperationCase<VectorFormat>) =
    [ let config = defaultConfig

      let wgSize = 32

      let context = case.TestContext.ClContext
      let q = case.TestContext.Queue

      q.Error.Add(fun e -> failwithf $"%A{e}")

      let getCorrectnessTestName datatype =
          sprintf "Correctness on %s, %A" datatype case.Format

      let boolOfList = Vector.ofList context wgSize

      let toCoo = Vector.toSparse context wgSize

      case
      |> correctnessGenericTest<bool> (=) boolOfList toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      let intOfList = Vector.ofList context wgSize

      let toCoo = Vector.toSparse context wgSize

      case
      |> correctnessGenericTest<int> (=) intOfList toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let byteOfList = Vector.ofList context wgSize

      let toCoo = Vector.toSparse context wgSize

      case
      |> correctnessGenericTest<byte> (=) byteOfList toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")

      let floatOfList = Vector.ofList context wgSize

      let toCoo = Vector.toSparse context wgSize

      case
      |> correctnessGenericTest<float> floatIsEqual floatOfList toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "float") ]

let tests =
    operationGPUTests "Backend.Vector.ofList tests" testFixtures
