// An application writing to one node: read the doc, then edit the winner
// (or, sometimes, a conflicting leaf), create, or delete. The read and the
// write are separate requests, so concurrent clients produce 409s, and
// clients on different nodes produce real conflicts once replicated.

type tClientConfig = (node: machine, cid: int, ops: int, docs: int, driver: machine);

machine Client {
  var cfg: tClientConfig;
  var counter: int;

  start state Init {
    entry (c: tClientConfig) {
      cfg = c;
      goto Running;
    }
  }

  state Running {
    entry {
      var i: int;
      while (i < cfg.ops) {
        EditOnce();
        i = i + 1;
      }
      send cfg.driver, eClientDone;
    }
  }

  fun EditOnce() {
    var doc: int;
    var got: tGetDocResp;
    var req: tEditReq;
    var ok: bool;
    doc = choose(cfg.docs);
    send cfg.node, eGetDoc, (client = this, doc = doc);
    receive {
      case eGetDocResp: (r: tGetDocResp) { got = r; }
    }
    counter = counter + 1;
    req.client = this;
    req.doc = doc;
    req.deleted = choose(4) == 0;
    if (!got.found) {
      req.hasParent = false;
      req.rev = (pos = 1, id = cfg.cid * 1000 + counter);
    } else {
      req.hasParent = true;
      // Usually edit the winner; sometimes resolve/extend another leaf.
      if ($) {
        req.parent = got.winner;
      } else {
        req.parent = choose(got.leaves);
      }
      req.rev = (pos = req.parent.pos + 1, id = cfg.cid * 1000 + counter);
    }
    send cfg.node, eEdit, req;
    receive {
      case eEditResp: (r: (ok: bool)) { ok = r.ok; }
    }
    if (ok) {
      announce eSpecEditAcked, (doc = doc, rev = req.rev);
    }
  }
}
