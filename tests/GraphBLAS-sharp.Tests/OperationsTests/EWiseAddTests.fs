module EWiseAddTests

open Expecto
open FsCheck
open GraphBLAS.FSharp
open MathNet.Numerics
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open GlobalContext
open TypeShape.Core
open GraphBLAS.FSharp.Tests

type PrimitiveType =
    | Float32
    | Bool

type PairOfSparseMatrices =
    static member Float32Type() =
        Arb.fromGen <| Generators.pairOfSparseMatricesGenerator
            Arb.generate<float32>
            0f
            ((=) 0f)

    static member BoolType() =
        Arb.fromGen <| Generators.pairOfSparseMatricesGenerator
            Arb.generate<bool>
            false
            ((=) false)

// type IPredicate =
//     abstract Invoke : 'a -> bool

// let equals (a: 'a) (b: 'a) = true

// let reflexivity = {
//     new IPredicate with
//         member this.Invoke item = equals item item
// }

// let conf = {
//     Config.Verbose with
//         Arbitrary = [typeof<Arbi>]
// }

// let meaning randomType =
//     match randomType with
//     | Float32 -> typeof<float32>
//     | Bool -> typeof<bool>

// let check (predicate: IPredicate) (randomType: PrimitiveTypes) =
//     let systemType = meaning randomType
//     let shape = TypeShape.Create systemType
//     shape.Accept {
//         new ITypeVisitor<bool> with
//             member this.Visit<'a>() =
//                 Check.One<'a -> bool>(conf, predicate.Invoke)
//                 true
//     }

// // генерится тип
// let checkGeneric (predicate: IPredicate) =
//     Check.Quick<PrimitiveTypes -> bool>(check predicate)
