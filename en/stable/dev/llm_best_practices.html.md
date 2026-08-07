<a id="lmm-best-practices"></a>

# Best practices for working with LLMs

* Do not use agents for reasoning about data. Reasoning requires domain
  expertise that LLMs do not have. Finalizing code that reasons about data often
  involves iterative exploration that is difficult to summarize & document
  effectively if you are not the one that did the iteration.
* Use agents to amplify a hand-built prototype: reduce tedium and identify edge
  cases you might not have seen.
* Prioritize solutions that you could replicate yourself: reinforce things you
  already understand, instead of creating a black box.
* Prioritize solutions you can maintain yourself: agentic programming is
  currently operated at a loss and that will not be true forever.
* Use agents to build tools for mass modifications, instead of having them do
  the mass modifications directly. A few dozen lines in a tool are easier for
  humans to evaluate for correctness than hundreds of line edits.
* Use agents to locate relevant analyses which have been written or vetted by
  human experts.
* Guarding against scope creep is easier to do at the beginning than while
  you’re in it: think carefully about the requirements and boundaries of a task
  before you bring an LLM into it.
* Relentlessly push your LLM to keep things simple. If something seems to your
  human judgement to be harder than it “should” be, follow that instinct.
