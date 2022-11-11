module Backend.Vector.OfList

open Expecto
open Expecto.Logging
open Brahma.FSharp
open GraphBLAS.FSharp.Tests.Utils
open GraphBLAS.FSharp.Backend
open OpenCL.Net

let logger = Log.create "Vector.ofList.Tests"

let checkResult (isEqual: 'a -> 'a -> bool) (expectedIndices: int []) (expectedValues: 'a []) (actual: Vector<'a>) =

    Expect.equal actual.Size (Array.max expectedIndices + 1) "lengths must be the same"

    match actual with
    | VectorSparse actual ->
        compareArrays (=) actual.Indices expectedIndices "indices must be the same"
        compareArrays isEqual actual.Values expectedValues "values must be the same"
    | _ -> failwith "Vector format must be Sparse."

let correctnessGenericTest<'a when 'a: struct>
    (isEqual: 'a -> 'a -> bool)
    (ofList: VectorFormat -> (int * 'a) list -> ClVector<'a>)
    (toCoo: MailboxProcessor<_> -> ClVector<'a> -> ClVector<'a>)
    (case: OperationCase<VectorFormat>)
    (elements: (int * 'a) [])
    =

    let elements =
        elements |> Array.distinctBy fst |> List.ofArray

    if elements.Length > 0 then

        let q = case.ClContext.Queue

        let indices, values =
            elements
            |> Array.ofList
            |> Array.sortBy fst
            |> Array.unzip

        let clActual = ofList case.Format elements

        let clCooActual = toCoo q clActual

        let actual = clCooActual.ToHost q

        clActual.Dispose q
        clCooActual.Dispose q

        checkResult isEqual indices values actual

let testFixtures (case: OperationCase<VectorFormat>) =
    [ let config = defaultConfig

      let wgSize = 32

      let context = case.ClContext.ClContext
      let q = case.ClContext.Queue

      q.Error.Add(fun e -> failwithf $"%A{e}")

      let getCorrectnessTestName datatype =
          sprintf "Correctness on %s, %A" datatype case.Format

      let boolOfList = Vector.ofList context

      let toCoo = Vector.toSparse context wgSize

      case
      |> correctnessGenericTest<bool> (=) boolOfList toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      let intOfList = Vector.ofList context

      let toCoo = Vector.toSparse context wgSize

      case
      |> correctnessGenericTest<int> (=) intOfList toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "int")


      let byteOfList = Vector.ofList context

      let toCoo = Vector.toSparse context wgSize

      case
      |> correctnessGenericTest<byte> (=) byteOfList toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")

      let floatOfList = Vector.ofList context

      let toCoo = Vector.toSparse context wgSize

      case
      |> correctnessGenericTest<byte> (=) floatOfList toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "float") ]

let tests =
    testsWithFixtures<VectorFormat> testFixtures "Backend.Vector.ofList tests"
