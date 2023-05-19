# [How Does a Database Work?](https://cstack.github.io/db_tutorial/) の Rust 実装

```
$ cargo run run.db
> insert 1 wass wass@example.com
Row { id: 1, name: wass, email: wass@example.com }
> insert 12 banana banana@example.com
Row { id: 12, name: banana, email: banana@example.com }
> insert 5 corn corn@example.coom
Row { id: 5, name: corn, email: corn@example.coom }
> .btree
Table { root_page_num: 0 }
Node 0 NodeType: Leaf, IsRoot: Yes, Parent: 0 ( NumCells: 3, NextLeaf 0 )
[1] Row { id: 1, name: wass, email: wass@example.com }
[5] Row { id: 5, name: corn, email: corn@example.coom }
[12] Row { id: 12, name: banana, email: banana@example.com }

> select 1
Row { id: 1, name: wass, email: wass@example.com }
```

# How Does a Database Work? の問題点

メジャーではない B+木の実装のために、様々な不具合がある。

- データの最大値を内部ノードのキーに採用。通常は最小値
  - ノードの編集時の不整合が起こりやすい
- 内部ノードの最後の最大値を持っていない。通常は最小値を持つ。
  - データ構造の変更の不一貫性で大変になる。
- ルートノードの ID を 0 に固定。通常は可変。
  - ルートノード分割で親ノードの変更にコストがかかる。

通常の実装に変更した。
